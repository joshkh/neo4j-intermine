(ns neomine.core
  (:require [neomine.mounts :refer [db]]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [cheshire.core :as cheshire]
            [imcljs.fetch :as fetch]
            [clojure.core.async :refer [<! go]]
            [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.labels :as nl]
            [clojurewerkz.neocons.rest.index :as ni]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [taoensso.timbre :refer [infof]]
            [clojure.pprint :refer [pprint]])
  (:gen-class))

(def testmine {:root  "http://beta.flymine.org/beta"
               :model {:name "genomic"}})

(defn fetch-classes [service]
  (-> service fetch/model :body (cheshire/parse-string true) :model :classes))

(defn fetch-model [service]
  (-> service fetch/model :body (cheshire/parse-string true) :model))


(defn fetch-session [service]
  (-> service fetch/session :body (cheshire/parse-string true) :token))

(defn remove-empty-keys [m] (into {} (filter second m)))

(defn add-label [node]
  (let [label (get-in node [:data :class])]
    (nl/add db node label)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn class-query [{:keys [name attributes]}]
  {:from   name
   :select (map (comp (partial str name ".") :name second) attributes)})

(defn exec-query-records [service query & [limit]]
  (:results (cheshire/parse-string (:body (fetch/records service query (when limit {:size limit}))) true)))

(defn exec-query-rows [service query & [limit]]
  (cheshire/parse-string (:body (fetch/rows service query (cond-> {:columnheaders "path"}
                                                                  limit (assoc :size 10)))) true))

(defn store-in-neo4j [maps & label]
  (doall (map add-label (nn/create-batch db maps))))

(defn load-class [service class & [limit]]
  (infof "Querying for: %s" (:name class))
  (let [results (exec-query-records testmine (class-query class) limit)]
    (infof "Inserting: %s : %s" (count results) (:name class))
    (store-in-neo4j (map remove-empty-keys results))
    (infof "Finished: %s" (:name class))
    (map :objectId results)))

(defn node-ids-by-label [label]
  (flatten (:data (cy/query db (format "MATCH (n:%s) RETURN n.objectId" label)))))

(defn load-genes []
  (let [classes (fetch-classes testmine)
        model   (fetch-model testmine)]
    (let [gene-ids (load-class testmine (:Gene classes) 3000)]
      (let [indexes (ni/get-all db "Gene")]
        (when-not (some? (some #{"objectId"} (mapcat :property_keys indexes)))
          (ni/create db "Gene" :objectId))))))

(def refs [; Order of operation matters!
           ; Load Homologues (these count as normal genes later in the queries)
           {:path              "Gene.homologues.homologue"
            :root              "Gene"
            :direction         :forward
            :relationship-name "HOMOLOGOUS_TO"}

           ; Load Gene -> Organisms
           {:path              "Gene.organism"
            :root              "Gene"
            :direction         :forward
            :relationship-name "IN_ORGANISM"}

           ; Load Gene <- Disease
           {:path              "Gene.diseases"
            :root              "Gene"
            :direction         :reverse
            :relationship-name "INVOLVES"}

           ; Load Gene <- Pathway
           {:path              "Gene.pathways"
            :root              "Gene"
            :direction         :reverse
            :relationship-name "CONTAINS_GENE"}

           ; Load Gene <- Transcription Factor (Regulatory Gene)
           {:path              "Gene.regulatoryRegions.factor"
            :root              "Gene"
            :direction         :reverse
            :relationship-name "REGULATES"
            :subclass          {:path "regulatoryRegions"
                                :type "TFBindingSite"}}

           ; Load Gene <- Protein Domain
           {:path              "Gene.proteins.proteinDomainRegions.proteinDomain"
            :root              "Gene"
            :direction         :reverse
            :relationship-name "DOMAIN_HAS"}

           ; Load Gene -> Chromsome (Casted from BioEntity)
           {:path              "Gene.chromosomeLocation.locatedOn"
            :root              "Gene"
            :direction         :forward
            :force-label       "Chromosome"
            :relationship-name "LOCATED_ON"
            :subclass          {:path "chromosomeLocation.locatedOn"
                                :type "Chromosome"}}

           ; Loading Gene -> Tissue
           {:path              "Gene.microArrayResults.tissue"
            :root              "Gene"
            :direction         :forward
            :force-label       "Tissue"
            :relationship-name "EXPRESSED_IN"
            :subclass          {:path "microArrayResults",
                                :type "FlyAtlasResult"}}

           ; Load Gene -> Phenotype
           {:path              "Gene.rnaiResults"
            :root              "Gene"
            :direction         :forward
            :force-label       "Phenotype"
            :relationship-name "HAS_PHENOTYPE"}
           ])

(defn reconstitute [views result-vector]
  (let [ks (map (comp keyword last #(clojure.string/split % #"\.")) views)]
    (clojure.set/rename-keys (remove-empty-keys (into {} (mapv (fn [k v] [k v]) ks result-vector))) {:id :objectId})))


(defn store-in-neo4j-without-label [maps & label]
  (doall (nn/create-batch db maps)))

(def cypher-relate-nodes
  "MATCH (s:%s {objectId: {sid}}),
         (d:%s {objectId: {did}})
  CREATE (s)-[:%s]->(d);")

(def cypher-relate-nodes-reverse
  "MATCH (s:%s {objectId: {sid}}),
         (d:%s {objectId: {did}})
  CREATE (s)<-[:%s]-(d);")

(defn relate [ref root end-class [source-id dest-id]]
  ;(println root end-class source-id dest-id)
  (cy/query db
            (if (= (:direction ref) :reverse)
              (format cypher-relate-nodes-reverse root end-class (:relationship-name ref))
              (format cypher-relate-nodes root end-class (:relationship-name ref)))
            {:sid source-id :did dest-id}))

(defn load-ref-objects [ref]
  (let [{:keys [path root direction relationship subclass force-label]} ref
        model (fetch-model testmine)]
    (let [ids             (node-ids-by-label root)
          end-class       (keyword (imcljs.path/class model path))
          _               (println (format "LOADING: %s %s %s (%s)" root direction (name end-class) path))
          query           {:from   root
                           :select (map #(str path "." (:name %)) (vals (get-in model [:classes end-class :attributes])))
                           :where  (cond-> [{:path (str root ".id") :op "ONE OF" :values ids}]
                                           subclass (conj subclass))}
          results         (exec-query-rows testmine query)
          reconstituted   (map (partial reconstitute (:views results)) (:results results))
          loaded          (store-in-neo4j-without-label reconstituted)
          mapping-query   {:from   root
                           :select [(str root ".id") (str path ".id")]
                           :where  (cond-> [{:path (str root ".id") :op "ONE OF" :values ids}]
                                           subclass (conj subclass))}
          mapping-results (:results (exec-query-rows testmine mapping-query))]
      (doall (map #(nl/add db % (or force-label (name end-class))) loaded))

      (let [indexes (ni/get-all db (or force-label (name end-class)))]

        (when-not (some? (some #{"objectId"} (mapcat :property_keys indexes)))
          (ni/create db (or force-label (name end-class)) :objectId)))

      (doall (map (partial relate ref root (or force-label (name end-class))) mapping-results))
      (println "Finished: " (or force-label (name end-class))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (mount/start)
  (let [session (fetch-session testmine)]
    (load-genes)
    (doall (map load-ref-objects refs))))


(ns neomine.relationships
  (:require [neomine.mounts :refer [db]]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [cheshire.core :as cheshire]
            [imcljs.fetch :as fetch]
            [clojure.core.async :refer [<! go]]
            [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.labels :as nl]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [taoensso.timbre :refer [infof]])
  (:gen-class))

(def testmine {:root "localhost:8080/biotestmine"
               :model {:name "genomic"}})


(defn build-relationship [node-1-oid node-1-label node-2-oid node-2-label relationship-name attributes]
  (format "MATCH (n:%s) {objectId: $objectId} RETURN n.objectId " node-1-label))



(defn dump-query [class]
  {:from (:name class)
   :select (map (fn [[k {:keys [name]}]]
                  (str (:name class) "." name ".id")) (:collections class))})

(defn dump-classold [service class]
  (println "DUMP" (dump-query class))
  (cheshire/parse-string (:body (fetch/rows service (dump-query class) {:size 10})) true))

(defn fetch-classes [service]
  (-> service fetch/model :body (cheshire/parse-string true) :model :classes))

(defn remove-empty-keys [m] (into {} (filter second m)))

(defn add-label [node]
  (let [label (get-in node [:data :class])]
    (nl/add db node label)))

(defn creater [nodes] (doall (map add-label (nn/create-batch db nodes))))

#_(defn load2 [c]
  (let [nodes (map remove-empty-keys (dump-class testmine c))]
    (clojure.pprint/pprint nodes)
    (infof "Loading: %s" (:name c))
    ;(doall (map creater (partition-all 100 nodes)))
    (infof "Finished")))

(defn build-query [class [_ {:keys [name]}]]
  {:from   class
   :select [(str class ".id")
            (str name ".id")]})

(defn build-collection-query [class]
  {:from ()})

(defn find-node-by-id [label oid]
  (cy/tquery db "MATCH (n:Exon) where n.objectId = {oid} RETURN n" {:oid oid}))

(defn dump-class [service query]
  (cheshire/parse-string (:body (fetch/rows service query)) true))

(defn create-rel [label1 oid1 label2 oid2]
  (format
    "MATCH (f:%s),(t:%s)
    WHERE f.objectId = %s AND t.objectId = %s
    CREATE (f)-[r:ANNOTEDBY]->(t)
    RETURN r"
    label1 label2 oid1 oid2))


(defn load-collections [[class-kw {:keys [name collections]}]]
  (println "LOADING COLLECTIONS")
  (doall (map (fn [collection]
                (let [[col-kw {:keys [referencedType]}] collection]
                  (println "ref" referencedType)
                  ;(println "Q" (build-query name collection))
                  (let [results (:results (dump-class testmine (build-query name collection)))]
                    ;(println "RESULTS" results)
                    (infof "Linking %s to %s [%s]" name referencedType (count results))
                    (doall (map (fn [[from to]]
                                  (cy/tquery db (create-rel name from referencedType to))

                            #_(cy/tquery db (create-rel name from referencedType to))) results))))) collections))
  #_(let [queries (map (partial build-query name) collections)]
    (doall (map (partial dump-class testmine) queries))))



(def skip [:Sequence :SequenceFeature :BioEntity])

(defn -main [& args]
  (mount/start)
  (doall (map load-collections (apply dissoc (fetch-classes testmine) skip))))

#_(defn load-nodes []
  (let [classes (apply dissoc (fetch-classes testmine) skip)]
    (doall (map (comp load2 second) (dissoc classes :Sequence :SequenceFeature :BioEntity)))))

;(defn -main
;  "I don't do a whole lot ... yet."
;  [& args]
;  (mount/start)
;  (time (load-nodes)))

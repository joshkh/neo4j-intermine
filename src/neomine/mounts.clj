(ns neomine.mounts
  (:require [mount.core :refer [defstate]]
            [clojurewerkz.neocons.rest :as nr]))

(defstate db
          :start (nr/connect "http://neo4j:intermine@localhost:7474/db/data/")
          :stop identity)
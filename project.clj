(defproject neomine "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]
                 [intermine/imcljs "0.1.16-SNAPSHOT"]
                 [clojurewerkz/neocons "3.1.0"]
                 [mount "0.1.11"]
                 [cheshire "5.7.1"]
                 [com.taoensso/timbre "4.10.0"]]
  :main ^:skip-aot neomine.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})

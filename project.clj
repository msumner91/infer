(defproject infer "0.1.0-SNAPSHOT"
  :description "Infer project"
  :url ""
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [commons-io "2.4"]
                 [clojure.java-time "0.3.1"]
                 [metasoarous/oz "1.6.0-alpha25"]
                 [ring/ring-core "1.8.1"]]
  :main ^:skip-aot infer.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})

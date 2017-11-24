(defproject riemann-generic "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[riemann-cond-dt "1.0.1"]]
  :main ^:skip-aot riemann-generic.core
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[riemann "0.2.14"]
                                  [org.clojure/clojure "1.8.0"]]}})

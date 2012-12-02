(defproject hirop "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-cljsbuild "0.2.9"]]
  :cljsbuild
  {:builds [{:source-path "src-cljs"
             :compiler {
                        :output-to "out-js/hirop.js"
                        ;;:optimizations :whitespace
                        :optimizations :simple
                        ;;:optimizations :advanced
                        :pretty-print false}}],
   :crossovers
   [hirop.core hirop.backend hirop.session hirop.stateful],
   :crossover-jar false,
   :crossover-path "src-cljs/crossovers"}
  :dependencies [[org.clojure/clojure "1.4.0"]])

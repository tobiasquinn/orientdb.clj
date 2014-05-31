(defproject orientdb.clj "0.1.0-SNAPSHOT"
  :description "Clojure wrapper for the OrientDB Java API."
  :url "https://github.com/eduardoejp/orientdb.clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype" "https://oss.sonatype.org/content/groups/public/"}
  :plugins [[codox "0.6.1"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.orientechnologies/orient-commons "1.6.6"]
                 [com.orientechnologies/orientdb-client "1.6.6"]
                 [com.orientechnologies/orientdb-core "1.6.6"]
                 [com.tinkerpop.blueprints/blueprints-orient-graph "2.4.0"]
                 [blueprints.clj "0.1.0-SNAPSHOT"]])

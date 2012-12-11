(defproject storm/trident-kestrel "0.0.5-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"twitter-public" "http://maven.twttr.com/"
                 }

  :dependencies [[com.twitter/finagle-kestrel "5.3.9"]
                 [com.twitter/finagle-commons-stats "5.3.9"]
                 ]

  :dev-dependencies [[storm "0.9.0-wip6"]
                     [org.clojure/clojure "1.4.0"]
                     ])


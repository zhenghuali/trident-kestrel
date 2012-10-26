(defproject trident-kestrel "0.0.3-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"twitter-public" "http://maven.twttr.com/"
                 }

  :dependencies [[com.twitter/finagle-kestrel "5.3.9"]
                 [com.twitter/finagle-commons-stats "5.3.9"]
                 ]

  :dev-dependencies [[storm "0.8.2-wip15"]
                     [org.clojure/clojure "1.4.0"]
                     ])


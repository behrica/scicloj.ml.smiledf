(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'org.scicloj/scicloj.ml.smiledf	)
(def version "0.0.1")
#_ ; alternatively, use MAJOR.MINOR.COMMITS:
(def version (format "1.0.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")

(defn- pom-template [version]
  [[:description "Librray to convert between Smile DataFrame and Tech ML Dataset"]
   [:url "https://github.com/behrica/scicloj.ml.smiledf"]
   [:licenses
    [:license
     [:name "GPL 3.0"]
     [:url "https://www.gnu.org/licenses/gpl-3.0.en.html"]]]
   [:developers
    [:developer
     [:name "Carsten Behring"]]]
   ])



(defn test "Run all the tests." [opts]
  (let [basis    (b/create-basis {:aliases [:test]})
        cmds     (b/java-command
                  {:basis      basis
                    :main      'clojure.main
                    :main-args ["-m" "cognitect.test-runner"]})
        {:keys [exit]} (b/process cmds)]
    (when-not (zero? exit) (throw (ex-info "Tests failed" {}))))
  opts)

(defn- jar-opts [opts]
  (let [basis (b/create-basis {})]
    ;;  (b/write-pom {:class-dir class-dir
    ;;                :lib lib
    ;;                :version version
    ;;                :basis basis
    ;;                :pom-data (pom-template version)
    ;;                :src-dirs ["src"]})
    
    (assoc opts
           :lib lib :version version
           :jar-file (format "target/%s-%s.jar" lib version)
           :scm {:tag (str "v" version)}
           :basis basis
           :class-dir class-dir
           :pom-data (pom-template version)
           :target "target"
           :src-dirs ["src"])))

(defn ci "Run the CI pipeline of tests (and build the JAR)." [opts]
  (test opts)
  (b/delete {:path "target"})
  (let [opts (jar-opts opts)]
    (println "\nWriting pom.xml...")
    (b/write-pom opts)
    (println "\nCopying source...")
    (b/copy-dir {:src-dirs ["resources" "src"] :target-dir class-dir})
    (println "\nBuilding JAR...")
    (b/jar opts))
  opts)

(defn install "Install the JAR locally." [opts]
  (let [opts (jar-opts opts)]
    (b/install opts))
  opts)

(defn deploy "Deploy the JAR to Clojars." [opts]
  (let [{:keys [jar-file] :as opts} (jar-opts opts)]
    (dd/deploy {:installer :remote :artifact (b/resolve-path jar-file)
                :pom-file (b/pom-path (select-keys opts [:lib :class-dir]))}))
  opts)

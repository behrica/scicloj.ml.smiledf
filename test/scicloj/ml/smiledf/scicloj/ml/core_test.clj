(ns scicloj.ml.smiledf.scicloj.ml.core-test
  (:require
   [clojure.test :refer [deftest is]]
   [scicloj.metamorph.ml.toydata :as data]
   [scicloj.ml.smiledf.core :as smiledf]
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.column :as col])
  (:import
   [smile.data.type StructField DataTypes]
   [smile.data.vector ValueVector]))

(defn- value-vector-equals? [^ValueVector vv-0 ^ValueVector vv-1]
  (java.util.Arrays/equals
   (.toStringArray vv-0)
   (.toStringArray vv-1)))

(defn- validate-round-trip [ds]
  (let [round-tripped

        (-> ds
            (smiledf/ds-to-df)
            (smiledf/df-to-ds))]
    (is (= ds round-tripped))))

(defn- assert-dfs-equal [df round-tripped]
  (assert (every? true?
                  (map
                   #(value-vector-equals? %1 %2)
                   (.columns df)
                   (.columns round-tripped)))))

(defn- validate-round-trip--inverse [df]
  (let [round-tripped

        (-> df
            (smiledf/df-to-ds)
            (smiledf/ds-to-df))]
    (assert-dfs-equal df round-tripped)))


(deftest roundtrip-inverse
  (validate-round-trip--inverse
   (smile.data.DataFrame.
    (into-array smile.data.vector.ValueVector
                [(smile.data.vector.StringVector. "s" (into-array String ["hello" "world"]))
                 (smile.data.vector.IntVector. "i" (int-array [1 2]))
                 (smile.data.vector.DoubleVector. "d" (double-array [1.0 2.0]))
                 (smile.data.vector.FloatVector. "f" (float-array [1.0 2.0]))
                 (smile.data.vector.LongVector. "l" (long-array [1 2]))
                 (smile.data.vector.ByteVector. "b" (byte-array [1 2]))

                 (smile.data.vector.BooleanVector.
                  (StructField. "bo" DataTypes/BooleanType)
                  (boolean-array [true false]))
                 (smile.data.vector.CharVector. "c" (char-array [\1 \2]))
                 (smile.data.vector.ShortVector. "sh" (short-array [1 2]))
                 (smile.data.vector.ObjectVector. "obj1" (object-array [:a 1]))
                 (smile.data.vector.ObjectVector. "obj2" (object-array [:a nil]))
                 ;(ValueVector/ordinal "ord" (into-array String [ "a" "b"]))
                 ]))))


(deftest test-validate-round-trip

  (validate-round-trip
   (->
    (data/iris-ds)
    (ds/rename-columns ["sepal_length" "sepal_width" "petal_length" "petal_width" "species"])))

  (validate-round-trip
   (->
    (data/breast-cancer-ds)
    (ds/rename-columns
     ["mean-radius"
      "mean-texture"
      "mean-perimeter"
      "mean-area"
      "mean-smoothness"
      "mean-compactness"
      "mean-concavity"
      "mean-concave-points"
      "mean-symmetry"
      "mean-fractal-dimension"
      "radius-error"
      "texture-error"
      "perimeter-error"
      "area-error"
      "smoothness-error"
      "compactness-error"
      "concavity-error"
      "concave-points-error"
      "symmetry-error"
      "fractal-dimension-error"
      "worst-radius"
      "worst-texture"
      "worst-perimeter"
      "worst-area"
      "worst-smoothness"
      "worst-compactness"
      "worst-concavity"
      "worst-concave-points"
      "worst-symmetry"
      "worst-fractal-dimension"
      "class"])))

  (validate-round-trip
   (ds/->dataset {"a" [true false true]
                  "a1" [true nil true]
                  "b" ["x" nil "z"]
                  "c" [1 2 3]
                  "d" [1.0 nil 2.0]
                  "e" [0.1 0.2 0.3]
                  "f" [nil 0.2 nil]
                  "g" [nil nil nil]
                  "h" [1 "x" 1.0]
                  "i" [[1 2] [3 4] [5 6]]
                  "c1" [1 nil 3]
                  "j" ["a" "b" "c"]
                  "j1" ["a" "b" nil]
                  "k" [\a \b \c]
                  "k1" [\a \b nil]
                  "l" (byte-array (map byte [0 1 2]))
                  "m" (short-array (map short [0 1 2]))
                  "n" [:a :b :c]
                  "o" [{:foo 1} {:bar 2} {:baz 3}]})))

(deftest specific-data
  (is
   (->
    (smiledf/ds-to-df (ds/->dataset {"k" [\a \b \c]}))
    .dtypes
    first
    .isChar))

  (is
   (->
    (smiledf/ds-to-df (ds/->dataset {"k" (byte-array (map byte [0 1 2]))}))
    .dtypes
    first
    .isByte))

  (is
   (->
    (smiledf/ds-to-df (ds/->dataset {"k" (short-array (map short [0 1 2]))}))
    .dtypes
    first
    .isShort)))


(deftest numeric-df
  (let [df
        (smile.data.DataFrame.
         (into-array ValueVector [(smiledf/col-as-value-vector (col/new-column "long" [1 2 3]))
                                  (smiledf/col-as-value-vector (col/new-column "int" (int-array [1 2 3])))
                                  (smiledf/col-as-value-vector (col/new-column "double" [10.1 20.8 30.4]))
                                  (smiledf/col-as-value-vector (col/new-column "float" (float-array [1.0 2.0 3.0])))
                                  (smiledf/col-as-value-vector (col/new-column "short" (short-array [1 2 3])))]))]
    (def df df)
    (is (= ["long" "int" "double" "float" "short"] (vec (.names df))))
    (is (= 1.0 (.getFloat df 0 0)))
    (is (= (float 10.1) (.getFloat df 0 2)))
    (is (= "1" (.getString df 0 0)))
    (is (= smile.data.DataFrame (class (.describe df))))
    (is (=  85.30000000000001 (.sum (.toMatrix df))))

    (is (false? (.. df (column "double") isNullable)))
    (is (false? (.. df (column "double") (isNullAt 0))))
    (is (= 0 (.. df (column "double") (getNullCount))))
    (is (= ["10.1" "20.8"] (vec (.. df (column "double") (get (int-array [0 1])) toStringArray))))
    (is (= [1 2 3] (iterator-seq (.. df (column "int") stream iterator))))
    (is (= [1 2 3] (iterator-seq (.. df (column "int") intStream iterator))))
    (is (= [1.0 2.0 3.0] (iterator-seq (.. df (column "int") doubleStream iterator))))

    (is (= [10.1 20.8 30.4] (iterator-seq (.. df (column "double") stream iterator))))
    (is (= [10 20 30] (iterator-seq (.. df (column "double") intStream iterator))))
    (is (= [1 2 3] (iterator-seq (.. df (column "short") stream iterator))))


    (is (= [1 2 3] (iterator-seq (.. df (column "long") stream iterator))))))


(deftest string-df
  (let [df
        (smile.data.DataFrame.
         (into-array ValueVector [(smiledf/col-as-value-vector (col/new-column "boolean" (boolean-array [true false true])))
                                  (smiledf/col-as-value-vector (col/new-column "char" [\1 \2 \3]))
                                  (smiledf/col-as-value-vector (col/new-column "byte" [1 2 3]))
                                  (smiledf/col-as-value-vector (col/new-column "string" ["x" "y" "z"]))
                                  (smiledf/col-as-value-vector (col/new-column "mixed" ["x" 1 true]))]))]

    (is (= "true" (.getString df 0 0)))
    (is (= "1" (.getString df 0 1)))
    (is (= \1 (.. df (column "char") (getChar 0))))
    (is (= 1 (.. df (column "byte") (getByte 0))))
    (is (= [true false true] (iterator-seq (.. df (column "boolean") stream iterator))))
    (is (= ["x" "y" "z"] (iterator-seq (.. df (column "string") stream iterator))))
    (is (= ["x" 1 true] (iterator-seq (.. df (column "mixed") stream iterator))))))


(deftest df-with-nulls
  (let [df
        (smile.data.DataFrame.
         (into-array ValueVector [(smiledf/col-as-value-vector (col/new-column
                                                                  "boolean"
                                                                  (boolean-array [true false true])
                                                                  {}
                                                                  #{0}))]))]

    (is (true? (.. df (column "boolean") isNullable)))
    (is (nil? (.. df (column "boolean") (get 0))))
    (is (= 1 (.. df (column "boolean") getNullCount)))
    (is (= true (.. df (column "boolean") anyNull)))
    (is (= true (.. df (column "boolean") (isNullAt 0))))
    (is (= [false false true] (stream-seq! (.. df (column "boolean") stream))))))

(deftest set-value
  (let [col (smiledf/col-as-value-vector (col/new-column [1 2 3]))]
    (-> col (.update 0 5))
    (is (= [5 2 3] (stream-seq! (.. col intStream))))))


(defn- assert-as-to-df-equal [df]
  (assert-dfs-equal
   (smiledf/ds-as-df  df)
   (smiledf/ds-to-df  df)))

(comment 

  (assert-as-to-df-equal
   (scicloj.metamorph.ml.rdatasets/fpp2-a10))
  )



(comment
  (->>
   (reduce
    dissoc
    (ns-publics (find-ns 'scicloj.metamorph.ml.rdatasets))
    ['fetch-dataset 'DAAG-bomregions2021 '_fetch-dataset 'doc-url->md])


   vals
   (mapv #(do
            (println %)
            (println (-> % list eval dataframe/ds-as-df .names vec))))))



(comment


  (defn validate-test-train [dataset]
    (assert
     (validate-round-trip--inverse
      (.train dataset)))
    (assert
     (validate-round-trip--inverse
      (.test dataset))))

  (defn validate-data [dataset]
    (assert
     (validate-round-trip--inverse
      (.data dataset))))

  (validate-test-train (smile.datasets.Abalone.))
  (validate-data (smile.datasets.Ailerons.))
  (validate-data (smile.datasets.AutoMPG.))
  (validate-data (smile.datasets.Bank32nh.))
  (validate-data (smile.datasets.BitcoinPrice.))





  (smile.data.vector.ValueVector/nominal "test"
                                         (into-array String ["a" "b" "c"])))


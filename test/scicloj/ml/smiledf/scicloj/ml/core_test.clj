(ns scicloj.ml.smiledf.scicloj.ml.core-test
  (:require
   [clojure.test :refer [deftest is]]
   [scicloj.metamorph.ml.toydata :as data]
   [scicloj.ml.smiledf.core :as dataframe]
   [tech.v3.dataset :as ds])
  (:import
   [smile.data.type StructField DataTypes]
   [smile.data.vector ValueVector]))

(defn value-vector-equals? [^ValueVector vv-0 ^ValueVector vv-1]
  (def vv-0 vv-0)
  (java.util.Arrays/equals
   (.toStringArray vv-0)
   (.toStringArray vv-1)))

(defn- validate-round-trip [ds]
  (let [round-tripped

        (-> ds
            (dataframe/ds->df)
            (dataframe/df->ds))]
    (is (= ds round-tripped))))

(defn- validate-round-trip--inverse [df]
  (let [round-tripped

        (-> df
            (dataframe/df->ds)
            (dataframe/ds->df))]
    (assert (every? true?
                    (map
                     #(value-vector-equals? %1 %2)
                     (.columns df)
                     (.columns round-tripped))))))


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
                 (ValueVector/ordinal "ord" (into-array String [ "a" "b"]))
                 ])))
  )


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
    (dataframe/ds->df (ds/->dataset {"k" [\a \b \c]}))
    .dtypes
    first
    .isChar))

  (is
   (->
    (dataframe/ds->df (ds/->dataset {"k" (byte-array (map byte [0 1 2]))}))
    .dtypes
    first
    .isByte))

  (is
   (->
    (dataframe/ds->df (ds/->dataset {"k" (short-array (map short [0 1 2]))}))
    .dtypes
    first
    .isShort)))


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
(ns scicloj.ml.smiledf.core
  (:require
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.column :as col]
   [tech.v3.datatype :as dt])

  (:import
   [java.util Arrays]
   [smile.data.type StructField DataTypes]
   [smile.data DataFrame]
   [smile.data.vector ValueVector IntVector NullableIntVector NullableBooleanVector
    ObjectVector NullableFloatVector NullableLongVector NullableCharVector NullableByteVector
    StringVector]))


(defn- value-vector->column [vv-column]
  (let [vv-type
        (.. vv-column dtype id name)
        col-name (.name vv-column)
        the-seq
        (case vv-type
          "Float" (stream-seq! (.doubleStream vv-column))
          "Double" (stream-seq! (.doubleStream vv-column))
          "Int" (stream-seq! (.intStream vv-column))
          "Boolean" (map #(case %
                            0 false
                            1 true
                            nil)
                         (stream-seq! (.intStream vv-column)))
          "Object" (stream-seq! (.stream vv-column))
          "Long"  (map long (stream-seq! (.doubleStream vv-column)))
          "String" (stream-seq! (.stream vv-column))
          "Char"  (map char (stream-seq! (.intStream vv-column)))
          "Byte" (stream-seq! (.intStream vv-column))
          "Short" (stream-seq! (.intStream vv-column)))]

    (if (contains? #{"Object","String"} vv-type)
      (ds/new-column col-name the-seq)

      (ds/new-column col-name the-seq {}
                     (if (.isNullable vv-column)
                       (stream-seq! (.stream (.getNullMask vv-column)))
                       nil)))))
(defn df->ds [df]
  (ds/new-dataset
   (map
    value-vector->column
    (.columns df))))


(defn- replace-nil [col replacement]
  (map
   #(if (nil? %)
      replacement
      %)
   col))

(defn- construct [klass & args]
  (cond
    ;; workaaround for https://github.com/haifengl/smile/issues/823
    (= smile.data.vector.BooleanVector klass)
    (smile.data.vector.BooleanVector.
     (StructField. (first args) DataTypes/BooleanType)
     (boolean-array (second args)))

    :else
    (clojure.lang.Reflector/invokeConstructor klass (into-array Object args))))


(def transformers
  {:int        [smile.data.vector.IntVector smile.data.vector.NullableIntVector int-array 0]
   :int16      [smile.data.vector.ShortVector smile.data.vector.NullableShortVector short-array 0]
   :int64      [smile.data.vector.LongVector smile.data.vector.NullableLongVector  long-array 0]
   :float64    [smile.data.vector.DoubleVector smile.data.vector.NullableDoubleVector double-array 0]
   :float32    [smile.data.vector.FloatVector smile.data.vector.NullableFloatVector  float-array 0]
   :char       [smile.data.vector.CharVector smile.data.vector.NullableCharVector  char-array \0]
   :boolean    [smile.data.vector.BooleanVector smile.data.vector.NullableBooleanVector boolean-array false]
   :string     [smile.data.vector.StringVector smile.data.vector.StringVector nil nil]
   :int8       [smile.data.vector.ByteVector smile.data.vector.NullableByteVector byte-array 0]})


(defn- col->value-vector [col]
  (let [null-mask (java.util.BitSet.)
        col-name (str (-> col meta :name))
        datatype (-> col meta :datatype)
        [clazz nullable-class array-fn replacement] (get transformers datatype
                                                         [smile.data.vector.ObjectVector smile.data.vector.ObjectVector nil nil])]
    (run!
     (fn [idx]
       (.set null-mask idx true))
     (ds/missing col))
    (if (nil? clazz)
      (throw (IllegalArgumentException. (format "datatype %s is not supported" datatype)))

      (case (str (.getName clazz))
        "smile.data.vector.ObjectVector" (ObjectVector. col-name (object-array col))
        "smile.data.vector.StringVector" (StringVector. col-name (into-array String col))


        (if (empty? (col/missing col))
          (construct
           clazz
           col-name
           (array-fn col))

          (construct
           nullable-class
           col-name
           (array-fn (replace-nil col replacement))
           null-mask))))))

(defn ds->df [ds]
  (DataFrame.
   (into-array ValueVector
               (map
                (fn [col]
                  (col->value-vector col))
                (ds/columns ds)))))


(defn filter-by-index [coll idxs]
  (keep-indexed #(when ((set idxs) %1) %2)
                coll))


(defn- typed-double-stream [col]
  (->
   (java.util.stream.DoubleStream/of (dt/->double-array col))
   (.mapToObj
    (reify
      java.util.function.DoubleFunction
      (apply [this i]
        i)))))

(defn- typed-int-stream [col]
  (->
   (java.util.stream.IntStream/of (dt/->int-array col))
   (.mapToObj
    (reify
      java.util.function.IntFunction
      (apply [this i]
        i)))))

(defn- col->stream [col]
  ;(def col (col/new-column "a" [1.0 2.0]))
  (->
   (java.util.stream.Stream/of (object-array (dt/->array col)))
   ;;  (.map
   ;;   (reify
   ;;     java.util.function.Function
   ;;     (apply [this i]
   ;;       ;(def i i)
   ;;       i)))
   ))

(defn col-as-value-vector [col]

  (reify ValueVector
    (get [this ^int i]
      ;(println :get :i i)
      (get col i))
    (field [this] (StructField. (name (-> col meta :name))
                                (case (-> col meta :datatype)

                                  :int64  DataTypes/LongType
                                  :float64 DataTypes/DoubleType
                                  :int32 DataTypes/IntType
                                  :float32 DataTypes/FloatType
                                  :int16 DataTypes/ShortType
                                  :boolean DataTypes/BooleanType
                                  :char DataTypes/CharType

                                  DataTypes/ObjectType)))
    (size [this] (count col))
    (getFloat [this i] (get col i))
    (getDouble [this i] (get col i))
    (getChar [this i] (get col i))
    (getByte [this i] (get col i))
    (stream [this] (col->stream col))
    (intStream [this] (do
                        (java.util.stream.IntStream/of (dt/->int-array col))))
    (isNullable [this] (not (empty? (col/missing col))))
    (isNullAt [this i] (col/is-missing? col i))
    (getNullCount [this] (count (vec (col/missing col))))
    (^ValueVector get  [this ^smile.util.Index index]
      (col-as-value-vector
       (col/select col (-> index .stream stream-seq!))))
    ( set [this i value] (dt/set-value! col i value))
    
    ))


(defn ds-as-df [dataset]
  (def dataset dataset)
  (DataFrame.
   (into-array ValueVector
               (mapv
                col-as-value-vector
                (ds/columns dataset)))))




(ns orientdb.clj.schema
  (:refer-clojure :exclude [class?])
  (:require (orientdb.clj [cluster :as ocluster]
                          [sql :as osql])
            [orientdb.clj :as o]
            [clojure.set :as set])
  (:import (com.orientechnologies.orient.core.metadata.schema OSchema
                                                              OClass OClass$INDEX_TYPE
                                                              OType
                                                              OProperty
                                                              OPropertyImpl)
           (com.orientechnologies.orient.core.db.document ODatabaseDocumentTx)
           (com.orientechnologies.orient.core.iterator OIdentifiableIterator))
  (:use [clojure.template :only [do-template]]
        [orientdb.clj :only [*db*]]))

(declare schema
         class?
         index!)

;; [Util]
(do-template [<sym> <class> <variants>]
  (def <sym>
    (let [variants <variants>]
      (->> variants
           (map #(-> % name (.replace "-" "") .toUpperCase
                     (->> (str (pr-str <class>) "/"))
                     read-string eval))
           (interleave variants)
           (apply hash-map))))
  
  ^:private keyword->index-type OClass$INDEX_TYPE
  [:dictionary :full-text :unique :not-unique :proxy]
  
  ^:private keyword->otype OType
  [:boolean :byte
   :short :integer :long
   :float :double :decimal
   :string
   :date :date-time
   :embedded :embedded-list :embedded-set :embedded-map
   :link :link-list :link-set :link-map
   :binary :custom :transient]
  )

(def ^:private otype->keyword (set/map-invert keyword->otype))

(let [+valid+    (set (keys keyword->otype))
      +embedded+ #{:embedded :embedded-list :embedded-set :embedded-map}
      +linked+   #{:link :link-list :link-set :link-map}]
  (def ^:private valid-otype? (comp boolean +valid+))
  (def ^:private primitive-otype? (comp boolean (set/difference +valid+ +embedded+ +linked+)))
  (def ^:private linked-or-embedded-type? (comp boolean (set/union +embedded+ +linked+))))

;; [Wrappers]
(do-template [<class>]
  (deftype <class> [x])
  
  OSchemaWrapper
  OClassWrapper
  OPropertyWrapper)

(defmacro ^:private defwrapper [wrapper wrapped-class wrapped-var
                                count seq
                                assoc-ex-msg
                                base-get base-assoc base-dissoc
                                attrs]
  (let [gets (->> attrs
                  (map (fn [[k v]] (let [[v] v] (if v [k v]))))
                  (filter identity))
        assocs (->> attrs
                    (map (fn [[k v]] (let [[_ v] v] (if v [k v]))))
                    (filter identity))
        dissocs (->> attrs
                     (map (fn [[k v]] (let [[_ _ v] v] (if v [k v]))))
                     (filter identity))]
    `(deftype ~wrapper [~(with-meta wrapped-var {:tag wrapped-class})]
       clojure.lang.IPersistentMap
       (~'assoc [~'self ~'k ~'v]
         (case ~'k
           ~@(mapcat identity assocs)
           ~@(if base-assoc [base-assoc]))
         ~'self)
       (~'assocEx [~'self ~'k v#]
         (if (.containsKey ~'self ~'k)
           (throw (IllegalStateException. ~assoc-ex-msg))
           (do (.assoc ~'self ~'k v#)
             ~'self)))
       (~'without [~'self ~'k]
         (case ~'k
           ~@(mapcat identity dissocs)
           ~@(if base-dissoc [base-dissoc]))
         ~'self)
       
       java.lang.Iterable
       (~'iterator [~'self] (.iterator ^Iterable (.seq ~'self)))
       
       clojure.lang.Associative
       (~'containsKey [~'self k#] (boolean (.valAt ~'self k#)))
       (~'entryAt [~'self k#] (if-let [v# (.valAt ~'self k#)] (clojure.lang.MapEntry. k# v#)))
       
       clojure.lang.IPersistentCollection
       ~@(if count [`(~'count [~'self] ~count)])
       (~'cons [~'self o#] (doseq [[k# v#] o#] (.assoc ~'self k# v#)) ~'self)
       (~'equiv [~'self o#] (.equals ~'self o#))
       
       ~@(if seq
           (let [[from [k v]] seq]
             ['clojure.lang.Seqable
              `(~'seq [~'self]
                      (let [seq# (for ~from (clojure.lang.MapEntry. ~k ~v))]
                        (if (empty? seq#)
                          nil
                          seq#)))]))
       
       clojure.lang.ILookup
       (~'valAt [~'self ~'k not-found#]
         (case ~'k
           ~@(mapcat identity gets)
           (or ~base-get not-found#)))
       (~'valAt [~'self k#] (.valAt ~'self k# nil))
       
       java.lang.Object
       (~'equals [~'self ~'obj]
         (and (instance? ~wrapped-class ~'obj)
              (= ~wrapped-var (~(symbol (str ".-" wrapped-var)) ~(with-meta 'obj {:tag wrapped-class})))))
       )))

(defwrapper OSchemaWrapper OSchema oschema
  (count (.getClasses oschema)) ;; Counting
  [[c (map #(OClassWrapper. %) (.getClasses oschema))] [(c :&name) c]] ;; Seq
  (str "Class " k " already exists in schema.") ;; Assoc error message.
  (if-let [oc (.getClass ^OSchema oschema ^String k)] ;; Get
    (OClassWrapper. oc))
  ;; Assoc
  (let [c (or (get self k)
              (OClassWrapper. (.createClass ^OSchema (.-oschema ^OSchemaWrapper (schema)) ^String k)))]
    (doseq [[k v] v]
      (assoc c k v)))
  (.dropClass oschema k) ;; Dissoc
  ;; Attrs
  {:&id      [(.getIdentity oschema)]
   :&version [(.getVersion oschema)]}
  )

(defwrapper OClassWrapper OClass oclass
  (.count oclass) ;; Counting
  ;; Seq
  [[p (map #(OPropertyWrapper. %) (.declaredProperties oclass))] [(p :&name) p]]
  (str "Property " k " already exists in class" (self :&name) ".") ;; Assoc error message.
  ;; Get
  (if-let [p (.getProperty oclass k)]
    (OPropertyWrapper. p))
  ;; Assoc
  (if-let [oprop (or (get self k)
                     (OPropertyWrapper. (if-let [linked (v :&linked)]
                                          (if (primitive-otype? linked)
                                            (.createProperty ^OClass oclass ^String k ^OType (keyword->otype (v :&type)) ^OType (keyword->otype linked))
                                            (.createProperty ^OClass oclass ^String k ^OType (keyword->otype (v :&type)) ^OClass (.oclass ^OClassWrapper linked)))
                                          (.createProperty oclass k (keyword->otype (v :&type))))))]
    (reduce (fn [p [k v]]
              (assoc p k v))
            oprop
            (dissoc v :&index)))
  (.dropProperty oclass k) ;; Dissoc
  ;; Attrs
  {:&abstract [(.isAbstract oclass)
               (.setAbstract oclass v)] 
   :&name [(.getName oclass)]
   :&short-name [(.getShortName oclass)
                 (.setShortName oclass v)]
   :&size [(.getSize oclass)]
   :&over-size [(.getOverSize oclass)
                (.setOverSize oclass v)]
   :&indices [(seq (.getIndexes oclass))]
   :&class-indices [(seq (.getClassIndexes oclass))]
   :&default-cluster [(.getDefaultClusterId oclass)
                      (.setDefaultClusterId oclass (ocluster/id v))]
   :&clusters [(seq (.getClusterIds oclass))]
   :&polymorphic-clusters [(seq (.getPolymorphicClusterIds oclass))]
   :&indexed-properties [(map #(OPropertyWrapper. %) (.getIndexedProperties oclass))]
   :&strict-mode  [(.isStrictMode oclass)
                   (.setStrictMode oclass v)]
   :&java-class [(.getJavaClass oclass)]
   :&parent [(if-let [parent (.getSuperClass oclass)]
               (OClassWrapper. parent))
             (let [^OClassWrapper super-class (if (string? v)
                                                (get (schema) v)
                                                v)]
               (.setSuperClass oclass (.-oclass super-class)))]
   }
  )

(defwrapper OPropertyWrapper OProperty oprop
  nil ;; Counting
  [[x [:&name :&full-name :&owner :&type :&linked :&min :&max :&regex :&mandatory :&nullable :&indices]] ;; Seq
   [x (self x)]]
  (str "The attribute " k " does not exist for properties.") ;; Assoc error message.
  nil ;; Get
  ;; Assoc
  (case k
    :&index (index! oprop v))
  (assoc self k nil) ;; Dissoc
  ;; Attrs
  {:&name [(keyword (.getName oprop))
           (.setName oprop v)]
   :&full-name [(.getFullName oprop)]
   :&owner [(OClassWrapper. (.getOwnerClass ^OPropertyImpl oprop))]
   :&type [(otype->keyword (.getType oprop))
           (.setType oprop (keyword->otype v))]
   :&linked [(or (if (.getLinkedClass oprop) (-> oprop .getLinkedClass .getName))
                 (if (.getLinkedType oprop) (otype->keyword (.getLinkedType oprop))))
             (cond (nil? v)             (do (.setLinkedType ^OPropertyImpl oprop nil)
                                          (.setLinkedClass ^OPropertyImpl oprop nil))
                   
                   (primitive-otype? v) (.setLinkedType ^OPropertyImpl oprop (keyword->otype v))
                   
                   :else                (.setLinkedClass ^OPropertyImpl oprop (.oclass ^OClassWrapper v)))]
   :&min [(.getMin oprop)
          (.setMin oprop (if v (str v)))]
   :&max [(.getMax oprop)
          (.setMax oprop (if v (str v)))]
   :&regex [(.getRegexp oprop)
            (.setRegexp oprop (if v (str v)))]
   :&mandatory [(.isMandatory oprop)
                (.setMandatory oprop (boolean v))]
   :&not-null [(.isNotNull oprop)
               (.setNotNull oprop v)]
   :&indices [(seq (.getAllIndexes oprop))]})

(do-template [<class> <wrapper> <field>]
  (do (extend-protocol o/Wrappable
        <class>
        (wrap [self]
          (new <wrapper> self)))
    (extend-protocol o/Unwrappable
      <wrapper>
      (unwrap [self]
        (<field> self))))

  OClass    OClassWrapper    .-oclass
  OProperty OPropertyWrapper .-oprop
  OSchema   OSchemaWrapper   .-oschema
  )

;; [Interface]
;; Predicates
(do-template [<pred> <class>]
  (defn <pred> "" [x]
    (instance? <class> x))
  schema?   OSchemaWrapper
  class?    OClassWrapper
  property? OPropertyWrapper)

;; Schema
(defn ^OSchemaWrapper schema "Returns the DB schema."
  []
  (-> *db* .getMetadata .getSchema OSchemaWrapper.))

(defn save! "Saves the schema."
  []
  (-> *db* .getMetadata .getSchema .save)
  nil)

;; Classes
(defn sub-class? "" [^OClassWrapper sub-class ^OClassWrapper super-class]
  (.isSubClassOf ^OClass (.oclass sub-class) ^OClass (.oclass super-class)))

(defn super-class? "" [super-class sub-class]
  (sub-class? sub-class super-class))

(defn add-cluster! "" [^OClassWrapper class cluster]
  (.addClusterId ^OClass (.oclass class) (ocluster/id cluster))
  nil)

(defn remove-cluster! "" [^OClassWrapper class cluster]
  (.removeClusterId ^OClass (.oclass class) (ocluster/id cluster))
  nil)

(defn browse-class "A lazy-seq of all the documents of the specified class."
  [class & [options]]
  (let [{:keys [^Boolean wrapped? ^Boolean polymorphic? ^String fetch-plan]} options
        ^OIdentifiableIterator iter (.browseClass *db* (if (string? class)
                                                         class
                                                         (class :&name))
                                                  polymorphic?)]
    (when fetch-plan
      (.setFetchPlan iter fetch-plan))
    (map (if wrapped?
           (comp o/wrap o/wrap)
           o/wrap)
         (iterator-seq iter))))

(defn truncate-class! "" [^OClassWrapper class]
  (.truncate ^OClass (.-oclass class)))

;; Properties
(defn indexed? "" [property]
  (.areIndexed ^OClass (.oclass ^OClassWrapper (property :&owner))
               ^"[Ljava.lang.String;" (into-array [(property :&name)])))

(defn index! "" [property index-type]
  (osql/execute! (str "CREATE INDEX " (property :&full-name) " " (-> index-type name (.replace "-" "") .toUpperCase))))

(defn unindex! "" [property]
  (osql/execute! (str "DROP INDEX " (property :&full-name))))

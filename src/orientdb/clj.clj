(ns orientdb.clj
  (:refer-clojure :exclude [load])
  (:require [blueprints.clj :as graph])
  (:use [clojure.template :only [do-template]])
  (:import (com.orientechnologies.orient.core.record ORecord)
           (com.orientechnologies.orient.core.record.impl ODocument
                                                          ORecordBytes)
           (com.orientechnologies.orient.core.id ORID
                                                 ORecordId
                                                 OClusterPosition OClusterPositionFactory)
           (com.orientechnologies.orient.core.intent OIntentMassiveRead
                                                     OIntentMassiveInsert)
           (com.orientechnologies.orient.core.db.document ODatabaseDocument
                                                          ODatabaseDocumentTx
                                                          ODatabaseDocumentPool)
           (com.orientechnologies.orient.client.remote OServerAdmin)
           (com.tinkerpop.blueprints.impls.orient OrientGraph
                                                  OrientVertex
                                                  OrientEdge)
           (com.orientechnologies.orient.core.db.graph OGraphDatabase
                                                       OGraphDatabasePool)
           (blueprints.clj VertexWrapper
                           EdgeWrapper)
           java.util.Map$Entry
           (java.io OutputStream)
           ))

(declare ->odata ->clj
         document
         document?)

;; [State]
(def ^:dynamic ^:no-doc *intent* nil)
(def ^:dynamic ^OGraphDatabase ^{:doc "The currently active DB connection."} *db*)

;; [Wrappers]
(deftype ODocumentWrapper [^ODocument odoc]
  clojure.lang.IPersistentMap
  (assoc [self k v]
    (.field odoc (name k) (->odata v))
    self)
  (assocEx [self k v]
    (if (.containsKey self k)
      (throw (IllegalStateException. "The field is already present."))
      (.assoc self k v)))
  (without [self k]
    (.removeField odoc (name k))
    self)
  
  java.lang.Iterable
  (iterator [self]
    (.iterator ^Iterable (.seq self)))
  
  clojure.lang.Associative
  (containsKey [self k]
    (.containsField odoc (name k)))
  (entryAt [self k]
    (if-let [v (.valAt self k)]
      (clojure.lang.MapEntry. k v)))
  
  clojure.lang.IPersistentCollection
  (count [self]
    (count (.fieldNames odoc)))
  (cons [self o]
    (doseq [[k v] o]
      (.assoc self k v))
    self)
  (equiv [self o]
    (.equals self o))
  
  clojure.lang.Seqable
  (seq [self]
    (for [k (.fieldNames odoc)]
      (clojure.lang.MapEntry. (keyword k)
                              (->clj (.field odoc k)))))
  
  clojure.lang.ILookup
  (valAt [self k not-found]
    (or (.valAt self k)
        not-found))
  (valAt [self k]
    (case k
      :&id      (.getIdentity odoc)
      :&class   (.field odoc "@class")
      :&version (.field odoc "@version")
      :&size    (.field odoc "@size")
      :&type    (.field odoc "@type")
      :&db      (.getDatabase odoc)
      (->clj (.field odoc (name k)))))
  
  java.lang.Object
  (equals [self o]
    (and (document? o)
         (= (get self :&id) (get o :&id))
         (= (get self :&version) (get o :&version))))
  )

(defprotocol Wrappable
  (wrap [self]))

(defprotocol Unwrappable
  (unwrap [self]))

(extend-protocol Wrappable
  ODocument
  (wrap [self]
    (ODocumentWrapper. self))
  ODocumentWrapper
  (wrap [^ODocumentWrapper self]
    (let [elem (.getElement graph/*db* (get self :&id))]
      (cond (instance? OrientVertex elem) (VertexWrapper. elem)
            (instance? OrientEdge elem)   (EdgeWrapper. elem)))))

(extend-protocol Unwrappable
  ODocumentWrapper
  (unwrap [^ODocumentWrapper self]
    (.-odoc self))
  VertexWrapper
  (unwrap [^VertexWrapper self]
    (ODocumentWrapper. (.-obj self)))
  EdgeWrapper
  (unwrap [^EdgeWrapper self]
    (ODocumentWrapper. (.-obj self))))

;; [Utils]
(defn ^:no-doc ->odata "Converts from Clojure data to OrientDB data."
  [x]
  (cond (document? x)              (.-odoc ^ODocumentWrapper x)
        (satisfies? Unwrappable x) (->odata (unwrap x))
        (set? x)                   (java.util.HashSet. ^java.util.List (map ->odata x))
        (map? x)                   (reduce (fn [^java.util.HashMap total [k v]]
                                             (.put total (->odata k) (->odata v)))
                                           (java.util.HashMap.)
                                           x)
        (coll? x)                  (map ->odata x)
        :else                      x))

(defn ^:no-doc ->clj "Converts from OrientDB data to Clojure data."
  [x]
  (cond (instance? ODocument x)      (ODocumentWrapper. x)
        (instance? java.util.Set x)  (set (map ->clj x))
        (instance? java.util.Map x)  (let [^java.util.Set entries (.entrySet ^java.util.Map x)]
                                       (reduce (fn [new-map ^java.util.Map$Entry entry]
                                                 (assoc new-map (->clj (.getKey entry)) (->clj (.getValue entry))))
                                               {}
                                               (iterator-seq (.iterator entries))))
        (instance? java.util.List x) (map ->clj x)
        :else                        x))

(defn ^:no-doc intent->obj [intent]
  (case intent
    :massive-read  (OIntentMassiveRead.)
    :massive-write (OIntentMassiveInsert.)
    nil            nil
    (throw (IllegalArgumentException. (str "Unknown intent: " (pr-str intent))))))

;; [Interface]
;; DB
(defn ^OGraphDatabase open-db "Returns an OGraphDatabase instance."
  [location user password]
  (.acquire (OGraphDatabasePool/global) location user password))

(do-template [<sym> <doc-str> <body>]
  (defn <sym> <doc-str>
    ([] (<sym> *db*))
    ([^ODatabaseDocumentTx db] <body>))
  
  close-db!  ""  (.close ^ODatabaseDocument db)
  drop-db!   ""  (.drop ^ODatabaseDocument db)
  db-open?   ""  (not (.isClosed ^ODatabaseDocument db))
  db-exists? ""  (if (string? db)
                   (recur (ODatabaseDocumentTx. ^String db))
                   (.exists ^ODatabaseDocument db))
  
  db-info "Information relevant to the DB as a map with keys #{:name, :url, :user, :status, :size, :clusters, :properties, :default-cluster-id}."
  {:name (.getName db)
   :url (.getURL db)
   :user (.getUser db)
   :status (.getStatus db)
   :size (.getSize db)
   :clusters (for [c (.getClusterNames *db*)]
               [(.getClusterIdByName *db* c)
                c
                (-> (.getClusterType *db* c) .toLowerCase keyword)])
   :properties (reduce (fn [new-map ^java.util.Map$Entry entry]
                         (assoc new-map (.getKey entry) (.getValue entry)))
                       {}
                       (iterator-seq (.getProperties db)))
   :default-cluster-id (.getDefaultClusterId db)
   }
  )

(defn create-db!
  "Creates a new database either locally or remotely if it doesn't already exist. Doesn't return a DB instance.
When passed an username and password, a connection to the remote server will be established to create the database."
  ([^String location]
     (when (not (db-exists? location))
       (.create (ODatabaseDocumentTx. location)))
     nil)
  ([^String location user password]
     (when (not (db-exists? location))
       (-> (OServerAdmin. location)
           (.connect user password)
           (.createDatabase "local")))
     nil))

(defmacro with-db "" [db & forms]
  `(binding [*db* ~db]
     (try ~@forms
       (finally (close-db! *db*)))))

(defmacro with-db+ "Same as with-db, but also opens a connection in the *db* var in blueprints.clj"
  [db & forms]
  `(binding [*db* ~db]
     (graph/with-db (OrientGraph. *db*)
       (try ~@forms
         (finally (close-db! *db*))))))


;; Transaction
(defmacro tx
  "Evaluates the given forms inside a transaction.
If an exception arises, the transaction will fail inmediately and do an automatic rollback.
The exception will be re-thrown for the user to catch it."
  [& forms]
  `(try (.begin *db*)
     (let [r# (do ~@forms)]
       (.commit *db*)
       r#)
     (catch Exception e#
       (.rollback *db*)
       (throw e#))))

;; Intent
(defmacro with-intent
  "'intent' must be in #{:massive-read, :massive-write, nil}.
Nil means \"no intent\"."
  [intent & body]
  `(let [intent# ~intent]
     (try (binding [*intent* intent#]
            (.declareIntent *db* (intent->obj intent#))
            ~@body)
       (finally (.declareIntent *db* (intent->obj *intent*))))))

;; Document
(do-template [<pred> <class>]
  (defn <pred> "" [x]
    (instance? <class> x))
  document? ODocumentWrapper
  id?       ORID
  blob?     ORecordBytes)

(defn save! "Saves a document."
  ([^ODocumentWrapper doc]
     (.save ^ODocument (.-odoc doc))
     doc)
  ([^ODocumentWrapper doc cluster-name|force-create]
     (.save ^ODocument (.-odoc doc) cluster-name|force-create)
     doc)
  ([^ODocumentWrapper doc ^String cluster-name ^Boolean force-create]
     (.save ^ODocument (.-odoc doc) cluster-name force-create)
     doc))

(defn document "A new document of the given class with the given attributes."
  ([^String class]       (ODocumentWrapper. (ODocument. class)))
  ([^String class attrs] (merge (document class)
                                attrs)))

(defn insert-documents!
  "Massively inserts documents into the database in an efficient manner.
The documents must be passed as a sequence of maps.
Each document must have a :&class key and an (optional) :&cluster key"
  [doc-maps]
  (with-intent :massive-write
    (let [!doc (ODocument. ^java.util.Map {})]
      (doseq [{:keys [^String &class ^String &cluster] :as d} doc-maps]
        (let [!doc* (doto !doc
                      .reset
                      (.setClassName &class)
                      (-> (.field (name k) (->odata v))
                          (->> (doseq [[k v] (dissoc d :&class :&cluster)]))))]
          (if &cluster
            (.save !doc* &cluster)
            (.save !doc*)))))))

(defn ^ORID id "Returns an ORID."
  [cluster ^long position]
  (ORecordId. (if (integer? cluster)
                cluster
                (.getClusterIdByName *db* cluster))
              (.valueOf OClusterPositionFactory/INSTANCE position)))

(defn cluster-id=> "An ORID's cluster ID."
  [^ORID id]
  (.getClusterId id))

(defn cluster-position=> "An ORID's cluster position."
  [^ORID id]
  (-> id .getClusterPosition .longValueHigh))

(defn load "Loads a document, given an ID."
  ([^ORID id]                    (ODocumentWrapper. (.load *db* id)))
  ([^ORID id ^String fetch-plan] (ODocumentWrapper. (.load *db* id fetch-plan))))

(defn delete!
  "Deletes a record if it is passed or if its ORID is passed.
If either a vector or an edge are passed, the necessary clean-up will be taken care of."
  [x]
  (cond (document? x) (let [^ODocument doc (.-odoc ^ODocumentWrapper x)]
                        (.delete doc))
        (id? x)       (delete! (load x))
        :else         (.delete ^ORecord x))
  nil)

(defn undo! "Undoes local changes to documents."
  [^ODocumentWrapper doc]
  (.undo ^ODocument (.-odoc doc))
  doc)

(defn ->map "" [doc]
  (merge {} doc))

;; Blobs
(defn ->blob "Returns an ORecordBytes blob with the given data."
  [bytes|input-stream]
  (if (instance? java.io.InputStream bytes|input-stream)
    (doto (ORecordBytes. *db*)
      (.fromInputStream bytes|input-stream))
    (ORecordBytes. *db* bytes|input-stream)))

(defn blob-> "Writes out the contents of an ORecordBytes blob to the given output stream."
  [^ORecordBytes blob ^OutputStream output-stream]
  (.toOutputStream blob output-stream)
  nil)

;; JSON
(defn ->json
  "Transforms the record to JSON, optionally specifying formatting settings.

Format settings are separated by comma. Available settings are:
  rid: exports the record's id as property \"@rid\".
  version: exports the record's version as property \"@version\".
  class: exports the record's class as property \"@class\".
  attribSameRow: exports all the record attributes in the same row.
  indent:<level>: Indents the output with the level specified. Defaults to 0.

Example: \"rid,version,class,indent:6\" exports record id, version and class properties
along with record properties using an indenting level equals of 6.

If a format is not specified, the default is \"rid,version,class,attribSameRow,indent:2\"."
  [^ODocumentWrapper doc & [String format]]
  (.toJSON ^ODocument (.-odoc doc) (or format "rid,version,class,attribSameRow,indent:2")))

(defn load-json! "Fills the record parsing the content in JSON format."
  [^ODocumentWrapper doc json]
  (.fromJSON ^ODocument (.-odoc doc) json)
  doc)

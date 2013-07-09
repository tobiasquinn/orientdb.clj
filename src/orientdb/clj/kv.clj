(ns orientdb.clj.kv
  (:require [orientdb.clj :as o])
  (:use clojure.template)
  (:import (com.orientechnologies.orient.core.id ORID)
           (com.orientechnologies.orient.core.index OIndex
                                                    OSimpleKeyIndexDefinition)
           (com.orientechnologies.orient.core.record.impl ODocument)
           (com.orientechnologies.orient.core.metadata.schema OClass$INDEX_TYPE
                                                              OType)
           com.orientechnologies.orient.core.sql.OCommandSQL
           com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
           (com.orientechnologies.orient.core.db.record OTrackedList
                                                        OTrackedMap
                                                        OTrackedSet)
           orientdb.clj.ODocumentWrapper
           (java.util Date
                      Calendar)))

(do-template [<class> <slot>]
  (deftype <class> [<slot>])
  
  OTrackedListWrapper olist
  OTrackedSetWrapper  oset
  OTrackedMapWrapper  omap)

;; [Utils]
(defn ^:private ->expiry-date [expiry]
  (if (instance? Date expiry)
    expiry
    (-> (Calendar/getInstance)
        (doto (.add Calendar/MILLISECOND expiry))
        .getTime)))

(defn ^:private expired? [^ODocument d]
  (if-let [expiry (.field d "e")]
    (.after (Date.) expiry)
    false))

(defn ^:private v<== [x]
  (condp instance? x
    ODocumentWrapper    (.-odoc ^ODocumentWrapper x)
    OTrackedListWrapper (.-olist ^OTrackedListWrapper x)
    OTrackedSetWrapper  (.-oset ^OTrackedSetWrapper x)
    OTrackedMapWrapper  (.-omap ^OTrackedMapWrapper x)
    java.util.List      (mapv v<== x)
    java.util.Set       (set (mapv v<== x))
    java.util.Map       (let [hm (java.util.HashMap.)]
                          (doseq [[k v] x]
                            (.put hm k (v<== v)))
                          hm)
    x))

(defn ^:private v==> [x]
  (condp instance? x
    OTrackedList (OTrackedListWrapper. x)
    OTrackedMap  (OTrackedMapWrapper. x)
    OTrackedSet  (OTrackedSetWrapper. x)
    ODocument    (ODocumentWrapper. x)
    x))

(defn ^:private ^ODocument odocument=> [bucket k fetch-plan]
  (if-let [item (.get (.-odict bucket) k)]
    (if (instance? ORID item)
      (.load o/*db* item fetch-plan)
      item)))

;; [Wrappers]
(deftype ODictionaryWrapper [odict]
  clojure.lang.IPersistentMap
  (assocEx [self k v]
    (if (.containsKey self k)
      (throw (IllegalStateException. (str "Key \"" k "\" is already present.")))
      (.assoc self k v)))
  (without [self k]
    (o/tx (when-let [rid ^ORID (.get odict k)]
            (.delete o/*db* rid)
            (.remove odict k))
      self))

  java.lang.Iterable
  (iterator [self]
    (.iterator ^Iterable (.seq self)))
  
  clojure.lang.Associative
  (assoc [self k v*]
    (let [[^String v e] (if (vector? v*)
                          v*
                          [v*])
          rid ^ORID (.get odict k)
          ^ODocument odoc (if rid
                            (.load o/*db* rid "*:-1")
                            (ODocument. ^java.util.Map {}))]
      (.field odoc "v" (v<== v))
      (when e
        (.field odoc "e" (->expiry-date e)))
      (if rid
        (.save odoc)
        (.put odict k odoc)))
    self)
  (containsKey [self k]
    (.contains odict k))
  (entryAt [self k]
    (if-let [v (.valAt self k)]
      (clojure.lang.MapEntry. k v)))

  clojure.lang.Counted
  (count [self]
    (.getKeySize odict))
  
  clojure.lang.IPersistentCollection
  (cons [self o]
    (doseq [[k v] o]
      (.assoc self k (v<== v)))
    self)
  (equiv [self o]
    (.equals self o))

  clojure.lang.Seqable
  (seq [self]
    (->> odict .keys .iterator iterator-seq (map #(.entryAt self %))))
  
  clojure.lang.ILookup
  (valAt [self k not-found]
    (if (.containsKey self k)
      (.valAt self k)
      not-found))
  (valAt [self k]
    (if-let [odoc (odocument=> self k "*:-1")]
      (if (expired? odoc)
        (do (dissoc k)
          nil)
        (v==> (.field odoc "v")))))
  
  clojure.lang.IFn
  (invoke [self k]    (.valAt self k))
  (invoke [self k nf] (.valAt self k nf))
  
  java.lang.Object
  (equals [self o]
    (and (instance? ODictionaryWrapper o)
         (.equals odict (.-odict ^ODictionaryWrapper o))))
  )

(deftype OTrackedListWrapper [^OTrackedList olist]
  clojure.lang.IPersistentCollection
  (cons [self o]
    (doto olist
      (.add (v<== o))
      .setDirty)
    self)
  (equiv [self o]
    (.equals self o))

  clojure.lang.IPersistentVector
  (assocN [self i v]
    (.assoc self i (v<== v))
    self)
  (length [self]
    (.count self))

  clojure.lang.ILookup
  (valAt [self i]           (.nth self i))
  (valAt [self i not-found] (.nth self i not-found))

  clojure.lang.Associative
  (assoc [self i v]
    (doto olist
      (.set i (v<== v))
      .setDirty))
  (containsKey [self i]
    (and (> i 0)
         (< i (.count self))))
  (entryAt [self i]
    (clojure.lang.MapEntry. i (.nth self i)))

  clojure.lang.Counted
  (count [self]
    (.size olist))

  clojure.lang.Indexed
  (nth [self i]
    (v==> (.get olist i)))
  (nth [self i not-found]
    (if (.containsKey self i)
      (.nth self i)
      not-found))

  java.lang.Iterable
  (iterator [self]
    (.iterator olist))

  clojure.lang.Seqable
  (seq [self]
    (->> olist .iterator iterator-seq))

  clojure.lang.IFn
  (invoke [self i]           (.nth self i))
  (invoke [self i not-found] (.nth self i not-found))

  java.lang.Object
  (equals [self o]
    (and (instance? OTrackedListWrapper o)
         (.equals olist (.-olist ^OTrackedListWrapper o))))
  )

(deftype OTrackedSetWrapper [^OTrackedSet oset]
  clojure.lang.Counted
  (count [self]
    (.size oset))

  clojure.lang.Seqable
  (seq [self]
    (->> oset .iterator iterator-seq (map o/wrap)))
  
  clojure.lang.IPersistentCollection
  (cons [self o]
    (doto oset
      (.add (v<== o))
      .setDirty)
    self)
  (equiv [self o]
    (.equals self o))

  clojure.lang.IPersistentSet
  (disjoin [self o]
    (doto oset
      (.remove (v<== o))
      .setDirty))
  (contains [self o]
    (.contains oset o))
  (get [self o]
    (if (.contains oset (v<== o))
      o))
  
  java.lang.Iterable
  (iterator [self]
    (.iterator ^Iterable (.seq self)))

  clojure.lang.IFn
  (invoke [self i]
    (.get self i))

  java.lang.Object
  (equals [self o]
    (and (instance? OTrackedSetWrapper o)
         (.equals oset (.-oset ^OTrackedSetWrapper o))))
  )

(deftype OTrackedMapWrapper [^OTrackedMap omap]
  clojure.lang.Counted
  (count [self]
    (.size omap))

  java.lang.Iterable
  (iterator [self]
    (.iterator ^Iterable (.seq self)))
  
  clojure.lang.Seqable
  (seq [self]
    (map #(.entryAt self %)
         (.keySet omap)))

  clojure.lang.ILookup
  (valAt [self k not-found]
    (if (.containsKey self k)
      (.valAt self k)
      not-found))
  (valAt [self k]
    (v==> (.get omap k)))
  
  clojure.lang.IFn
  (invoke [self k]    (.valAt self k))
  (invoke [self k nf] (.valAt self k nf))

  clojure.lang.IPersistentCollection
  (cons [self o]
    (doseq [[k v] o]
      (.assoc self k v))
    self)
  (equiv [self o]
    (.equals self o))

  clojure.lang.Associative
  (assoc [self k v]
    (doto omap
      (.put k (v<== v))
      .setDirty))
  (containsKey [self k]
    (.containsKey omap k))
  (entryAt [self k]
    (clojure.lang.MapEntry. k (.valAt self k)))

  clojure.lang.IPersistentMap
  (assocEx [self k v]
    (if (.containsKey self k)
      (throw (IllegalStateException. (str "Key \"" k "\" is already present.")))
      (.assoc self k v)))
  (without [self k]
    (doto omap
      (.remove k)
      .setDirty))

  java.lang.Object
  (equals [self o]
    (and (instance? OTrackedMapWrapper o)
         (.equals omap (.-omap ^OTrackedMapWrapper o))))
  )

;; [Interface]
;; Macro for easy string interpolation
(defmacro <<
  "An small macro to do simple string interpolation, useful for generating keys.
Example: \"user/~|(+ 1 (* 2 3))|/email\""
  [string]
  (let [str-forms (re-seq #"~\|([^\|]*)\|" string)
        embedded-forms (map #(-> % second read-string)
                            str-forms)
        [str-rest parts] (reduce (fn [[^String str-rest parts] [form ^String str-form]]
                                   (let [i (.indexOf str-rest str-form)]
                                     [(.substring str-rest (+ i (count str-form)))
                                      (conj parts (.substring str-rest 0 i) form)]))
                                 [string []]
                                 (->> str-forms
                                      (map first)
                                      (interleave embedded-forms)
                                      (partition 2)))]
    `(str ~@parts ~@(if-not (empty? str-rest)
                      [str-rest]))))

;; Buckets
(defn create-bucket! "" [^String bname]
  (-> o/*db*
      .getMetadata
      .getIndexManager
      (.createIndex bname (.toString OClass$INDEX_TYPE/DICTIONARY)
                    (OSimpleKeyIndexDefinition. (into-array [OType/STRING]))
                    nil nil)
      ODictionaryWrapper.))

(defn fetch-bucket "" [^String bname]
  (if-let [odict (-> o/*db*
                     .getMetadata
                     .getIndexManager
                     (.getIndex bname))]
    (ODictionaryWrapper. odict)))

(defn delete-bucket! "" [^OIndex bucket]
  (-> o/*db*
      .getMetadata
      .getIndexManager
      (.dropIndex (.getName bucket)))
  nil)

(defn clear! "Removes all keys from the current bucket."
  [^ODictionaryWrapper bucket]
  (.clear (.-odict bucket))
  bucket)

(defn unload! "Unloads the current bucket to free memory."
  [^ODictionaryWrapper bucket]
  (.unload (.-odict bucket))
  nil)

;; K/Vs
(defn matching-keys
  "Returns all keys in the current bucket that match the given pattern (specified as either a RegEx or a String)."
  [^ODictionaryWrapper bucket pattern]
  (let [q (OSQLSynchQuery. (str "SELECT key FROM index:" (.getName (.-odict bucket)) " WHERE key MATCHES ?"))]
    (->> (.query o/*db* q (to-array [(str pattern)]))
         (map #(.field ^ODocument % "key")))))

;; K/V expiration
(defn expire! "'expiry' can be a java.util.Date or a delay in milliseconds (counting from the current time)."
  [^ODictionaryWrapper bucket ^String k expiry]
  (boolean (if-let [odoc (odocument=> bucket k "e:1")]
             (doto odoc
               (.field "e" (->expiry-date expiry))
               .save))))

(defn persist! "" [^ODictionaryWrapper bucket k]
  (boolean (if-let [odoc (odocument=> bucket k "e:1")]
             (doto odoc
               (.removeField "e")
               .save))))

(defn refresh! "" [^ODictionaryWrapper bucket k new-expiry]
  (boolean (if-let [odoc (odocument=> bucket k "*:-1")]
             (doto odoc
               (.field "e" (->expiry-date new-expiry))
               .save))))

(defn ^Date expiry "Returns the expiration date on the given key as a java.util.Date object."
  [^ODictionaryWrapper bucket k]
  (if-let [odoc (odocument=> bucket k "e:1")]
    (.field odoc "e")))

(defn expires? "" [^ODictionaryWrapper bucket k]
  (boolean (expiry bucket k)))

(defn persistent? "" [^ODictionaryWrapper bucket k]
  (not (expires? bucket k)))

(defn ttl "Returns the time-to-live (long) left on the given key as-of the current time."
  [^ODictionaryWrapper bucket k]
  (if-let [e (expiry bucket k)]
    (- (.getTime e) (.getTime (Date.)))))

(defn remove-expired-keys! "" [^ODictionaryWrapper bucket]
  (.execute (.command o/*db* (OCommandSQL. (str "DELETE FROM index:" (.getName (.-odict bucket)) " WHERE e < ?")))
            (into-array [(Date.)]))
  bucket)

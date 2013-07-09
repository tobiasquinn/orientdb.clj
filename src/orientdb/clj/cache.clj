(ns orientdb.clj.cache
  (:require orientdb.clj)
  (:use [clojure.template :only [do-template]])
  (:import orientdb.clj.ODocumentWrapper
           (com.orientechnologies.orient.core.record.impl ODocument)))

;; [Interface]
(do-template [<sym> <method>]
  (defn <sym> "" [^ODocumentWrapper doc]
    (<method> ^ODocument (.-odoc doc))
    doc)
  
  pin!    .pin
  unpin!  .unpin
  unload! .unload
  )

(defn pinned? "" [^ODocumentWrapper doc]
  (.isPinned ^ODocument (.-odoc doc)))

(defn reload! "Reloads the record content."
  [^ODocumentWrapper doc & [^String fetch-plan ignore-cache]]
  (.reload ^ODocument (.-odoc doc) fetch-plan (if (nil? ignore-cache)
                                                true
                                                ignore-cache))
  doc)

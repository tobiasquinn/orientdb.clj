(ns orientdb.clj.hook
  (:require [orientdb.clj :as o])
  (:import orientdb.clj.ODocumentWrapper
           (com.orientechnologies.orient.core.record.impl ODocument)
           (com.orientechnologies.orient.core.hook ORecordHookAbstract
                                                   ORecordHook$RESULT)))

;; [Utils]
(def ^:private ->trigger
  '{before-create onRecordBeforeCreate after-create onRecordAfterCreate
    before-read   onRecordBeforeRead   after-read   onRecordAfterRead
    before-update onRecordBeforeUpdate after-update onRecordAfterUpdate
    before-delete onRecordBeforeDelete after-delete onRecordAfterDelete})

(def ^:no-doc ->result
  {:changed     ORecordHook$RESULT/RECORD_CHANGED
   :not-changed ORecordHook$RESULT/RECORD_NOT_CHANGED
   :skip        ORecordHook$RESULT/SKIP})

;; [Interface]
(defn hooks "" []
  (seq (.getHooks o/*db*)))

(defn add! "" [hook]
  (.registerHook o/*db* hook)
  nil)

(defn remove! "" [hook]
  (.unregisterHook o/*db* hook)
  nil)

(defn remove-all! "" []
  (doseq [hook (hooks)]
    (remove! hook)))

(defmacro hook
  "Creates a new hook from the following fn definitions (each one is optional):
  (before-create [~document] ~@body)
  (before-read [~document] ~@body)
  (before-update [~document] ~@body)
  (before-delete [~document] ~@body)
  (after-create [~document] ~@body)
  (after-read [~document] ~@body)
  (after-update [~document] ~@body)
  (after-delete [~document] ~@body)

All the before-* hooks can return a value that will be taken into account by the DB.
  :changed     <- means the record has changed.
  :not-changed <- means the record has not changed.
  :skip        <- means the record can be skipped.
  All values which are not any of these will be interpreted as :changed.

The after-* hooks' return values won't affect the DB and can be anything."
  [& triggers]
  {:pre [(->> triggers
              (map first)
              (every? '#{before-create after-create before-read after-read
                         before-update after-update before-delete after-delete}))]}
  `(proxy [ORecordHookAbstract] []
          ~@(for [[meth [arg] & forms] triggers]
              `(~(->trigger meth) [doc#]
                (if (instance? ODocument doc#) 
                  (let [~arg (ODocumentWrapper. doc#)]
                    (or (->result (do ~@forms))
                        com.orientechnologies.orient.core.hook.ORecordHook$RESULT/RECORD_CHANGED))
                  com.orientechnologies.orient.core.hook.ORecordHook$RESULT/RECORD_CHANGED)))))

(defmacro defhook
  "Defines a hook and creates a global binding for it (with an optional doc-string).
Note: The hook is NOT automatically addded to the DB."
  [sym & triggers]
  (let [[[doc-string] triggers] (split-with string? triggers)]
    `(def ~(with-meta sym {:doc doc-string})
       (hook ~@triggers))))

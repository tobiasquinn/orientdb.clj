(ns orientdb.clj.script
  (:refer-clojure :exclude [eval])
  (:use [orientdb.clj :only [*db*]])
  (:import com.orientechnologies.orient.core.command.script.OCommandScript))

(defn eval
  "Evals the given script on the current DB.
'type' must be a String specifying the language. Defaults to \"Javascript\"."
  ([type script]
     (-> *db*
         (.command (OCommandScript. type script))
         (.execute (object-array []))))
  ([script]
     (eval "Javascript" script)))

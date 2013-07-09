(ns orientdb.clj.sql
  (:use [orientdb.clj :only [*db* ->odata wrap]]
        clojure.template)
  (:import (com.orientechnologies.orient.core.sql OSQLEngine
                                                  OCommandSQL)
           (com.orientechnologies.orient.core.sql.query OSQLSynchQuery
                                                        OSQLAsynchQuery)
           (com.orientechnologies.orient.core.sql.functions OSQLFunction
                                                            OSQLFunctionAbstract)
           (com.orientechnologies.orient.core.command OCommandResultListener)
           orientdb.clj.ODocumentWrapper))

;; [Util]
(defn ^:private ->query-params [params]
  (to-array (if (map? params)
              [(->odata params)]
              (map ->odata params))))

(defn ^:private paginate "Returns a lazy-seq of concatenated batches of paginated results."
  [query params wrapper]
  (let [res (.query *db* query (->query-params params))]
    (if (empty? res)
      '()
      (lazy-cat (map wrapper res)
                (paginate query params wrapper)))))

(defn ^:private ->arity [arity]
  (->> arity
       (map #(str "<" % ">"))
       (interpose ",")
       (apply str)))

(defn ^:private ->syntax [sym arity]
  (let [[mandatory [_ optional]] (split-with #(not= '& %) arity)]
    (str sym "(" (->arity mandatory)
         (cond (symbol? optional) (str "[,<" optional "*>]")
               (seq? optional)    (str "[," (->arity optional) "]")
               :else nil)
         ")")))

(defn ^:private count-arity [arity]
  (loop [[mandatory [_ optional]] (split-with #(not= '& %) arity)
         in-rest? false
         min-so-far 0
         max-so-far 0]
    (cond (nil? optional)
          (if in-rest?
            [min-so-far (+ max-so-far (count mandatory))] ;; [a b c & [d e f]]
            [(count mandatory) (count mandatory)]) ;; [a b c]
          
          (symbol? optional)
          (if in-rest?
            [min-so-far Integer/MAX_VALUE] ;; [a b c & [d e f & rest]]
            [(count mandatory) Integer/MAX_VALUE]) ;; [a b c & rest]
          
          (vector? optional)
          (if in-rest?
            (recur (split-with #(not= '& %) optional) ;; [a b c & [d e f & [...]]]
                   true
                   min-so-far
                   (+ min-so-far (count mandatory)))
            (recur (split-with #(not= '& %) optional) ;; [a b c & [...]]
                   true
                   (count mandatory)
                   (count mandatory)))
          )))

(defn ^:private ->fn [sym arities type bodies]
  {:pre [(every? (complement map?) arities)]}
  (let [mm-counts (map count-arity arities)
        [min-arity] (sort < (map first mm-counts))
        [max-arity] (sort > (map second mm-counts))
        g!res (gensym "result")]
    `(let [fn-body# (fn ~sym ~@bodies)]
       (proxy [OSQLFunctionAbstract] [~(name sym) ~min-arity ~max-arity]
              (~'getName [] ~(name sym))
              (~'getMinParams [] ~min-arity)
              (~'getMaxParams [] ~max-arity)
              (~'getSyntax [] ~(->> arities
                                    (map #(->syntax sym %))
                                    (interpose " | ")
                                    (apply str "Syntax error: ")))
              (~'execute [document# current-result# args# ~'&ctx]
                (let [~'&document (ODocumentWrapper. document#)
                      ~g!res (apply fn-body# args#)]
                  ~(if (= type :filter)
                     `(if ~g!res
                        ~g!res)
                     g!res)))
              (~'filterResult [] ~(= type :filter))
              (~'aggregateResults [] false)
              ))))

(defn ^:private ->result-listener [callback]
  (reify OCommandResultListener
    (result [self record]
      (callback record))))

;; [Interface]
(defn query
  "Executes the given SQL query with the given parameters (as a Clojure vector or map)
and :fetch-plan and :paginate? options.

When using positional parameters (?), use a vector.
When using named parameters (:param), use a map.

If passed a callback, the query will be run asynchronously.
The callback must be a function of 1 argument that will be run once for each matching result.

The :paginate? option will only work on synchronous queries."
  [query & [params options callback]]
  (let [{:keys [^String fetch-plan ^Boolean paginate? wrapped]} options
        query* (let [^OSQLAsynchQuery query* (if callback
                                               (OSQLAsynchQuery. query (->result-listener callback))
                                               (OSQLSynchQuery. query))]
                 (doto query*
                   (.setFetchPlan fetch-plan)))
        wrapper (if wrapped
                  #(wrap (ODocumentWrapper. %))
                  #(ODocumentWrapper. %))
        res (map wrapper
                 (.query *db* query* (->query-params params)))]
    (if (and paginate?
             (not callback))
      (lazy-cat res (paginate query* params wrapper))
      res)))

(defn execute! "Runs the given SQL command (with optional arguments like those of query)."
  [command & [params]]
  (-> *db*
      (.command (OCommandSQL. command))
      (.execute (->query-params params)))
  nil)

;; SQL fns
(defn install-fn! "Installs a SQL function into the database (one defined using defsqlfn or defsqlfilter)"
  [^OSQLFunction sql-fn]
  (.registerFunction (OSQLEngine/getInstance) (.getName sql-fn) ^OSQLFunction sql-fn)
  nil)

(defn uninstall-fn! "" [^OSQLFunction sql-fn]
  (.unregisterFunction (OSQLEngine/getInstance) (.getName sql-fn))
  nil)

(do-template [<sym> <type> <doc-str>]
  (defmacro <sym> <doc-str>
    [sym & body]
    (let [[arities bodies] (if (vector? (first body))
                             [(list (first body)) (list body)]
                             [(map first body) body])]
      `(def ~sym ~(->fn sym arities <type> bodies))))
  
  defsqlfn :normal
  "Defines a regular SQL function that can be installed on the SQL engine.
Besides the arguments in it's arity, it will also receive the implicit arguments &document and &ctx,
of types ODocumentWrapper and OCommandContext respectively.

The function can have multiple arities and rest (&) parameters.
The parameters must NOT include map destructuring, but they can include sequence destructuring."

  defsqlfilter :filter
  "Defines a SQL function used for filtering.
The same rules that apply to 'defsqlfn' apply to defsqlfilter."
  )

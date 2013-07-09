(ns orientdb.clj.cluster
  (:refer-clojure :exclude [count name type])
  (:require [orientdb.clj :as o])
  (:use clojure.template)
  (:import com.orientechnologies.orient.core.iterator.OIdentifiableIterator
           com.orientechnologies.orient.core.storage.OStorage$CLUSTER_TYPE
           orientdb.clj.ODocumentWrapper))

;; [Utils]
(def ^:private keyword->cluster-type
  {:physical OStorage$CLUSTER_TYPE/PHYSICAL
   :memory   OStorage$CLUSTER_TYPE/MEMORY})

;; [Interface]
(defn ^Integer id "" [cluster]
  (if (integer? cluster)
    cluster
    (.getClusterIdByName o/*db* cluster)))

(defn ^String name "" [cluster]
  (if (string? cluster)
    cluster
    (.getClusterNameById o/*db* cluster)))

(do-template [<sym> <method>]
  (defn <sym> "" [^String cluster]
    (<method> o/*db* cluster))
  drop!   .dropCluster
  exists? .existsCluster)

(defn type "" [cluster]
  (-> (.getClusterType o/*db* (name cluster))
      .toLowerCase
      keyword))

(defn size "" [cluster]
  (.getClusterRecordSizeById o/*db* (id cluster)))

(defn browse "Returns a lazy-seq of all the documents in the specified cluster."
  [cluster & [options]]
  (let [{:keys [^Boolean wrapped? ^String fetch-plan]} options
        ^OIdentifiableIterator iter (.browseCluster o/*db* (name cluster))]
    (when fetch-plan
      (.setFetchPlan iter fetch-plan))
    (map (if wrapped?
           (comp o/wrap o/wrap)
           o/wrap)
         (iterator-seq iter))))

(defn count ""
  ([cluster]            (.countClusterElements o/*db* ^Integer (id cluster)))
  ([cluster & clusters] (.countClusterElements o/*db* ^ints (into-array (map id (cons cluster clusters))))))

(defn add! "type in #{:physical, :memory}."
  [cluster type & [location data-segment]]
  {:pre [(#{:physical, :memory} type)]}
  (if (and location data-segment)
    (.addCluster o/*db* (name cluster) (keyword->cluster-type type) (into-array []))
    (.addCluster o/*db* (str (keyword->cluster-type type)) (name cluster) location data-segment (into-array []))))

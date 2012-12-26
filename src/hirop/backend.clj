(ns hirop.backend)

(def ^:dynamic *connection-data* nil)

(defmulti fetch (fn [backend context] backend))

(defmulti save (fn [backend context] backend))

(defmulti history (fn [backend id] backend))


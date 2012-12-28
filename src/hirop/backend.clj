(ns hirop.backend)

(defmulti fetch (fn [backend context] (keyword (:name backend))))

(defmulti save (fn [backend context] (keyword (:name backend))))

(defmulti history (fn [backend id] (keyword (:name backend))))


(ns hirop.session
  "Middleware for working with 'stateful' sessions.
   Modified from noir session."
  (:refer-clojure :exclude [get get-in pop!]))

(declare ^:dynamic *hirop-session*)

(defn ^:private hirop-session [handler]
   "Store hirop session keys in a :hirop map, because other middleware that
   expects pure functions may delete keys, and simply merging won't work.
   Ring takes (not (contains? response :session) to mean: don't update session.
   Ring takes (nil? (:session resonse) to mean: delete the session.
   Because hirop-session mutates :session, it needs to duplicate ring/wrap-session
   functionality to handle these cases."
   (fn [request]
    (binding [*hirop-session* (atom (clojure.core/get-in request [:session :hirop] {}))]
      (swap! *hirop-session* dissoc :_flash)
      (when-let [resp (handler request)]
        (if (=  (clojure.core/get-in request [:session :hirop] {})  @*hirop-session*)
          resp
          (if (contains? resp :session)
            (if (nil? (:session resp))
              resp
              (assoc-in resp [:session :hirop] @*hirop-session*))
            (assoc resp :session (assoc (:session request) :hirop @*hirop-session*))))))))

(defn wrap-hirop-session*
  "A stateful layer around wrap-session. Expects that wrap-session has already
   been used."
  [handler]
  (hirop-session handler))

;;(defn update-session! [update-fn value]
;;  (swap! *hirop-session* update-fn value))

(defn update!
  [f & args]
  (apply (partial swap! *hirop-session* f) args))

(defn update-in!
  [ks f & args]
  (swap! *hirop-session*
         (fn [session]
           (apply (partial update-in session ks f) args))))

(defn put! [k v]
  (swap! *hirop-session* (fn [a b] (merge a {k b})) v))

(defn session-map []
  @*hirop-session*)

(defn get
  ([k] (get k nil))
  ([k default] (clojure.core/get @*hirop-session* k default)))

(defn get-in
  ([k] (get-in k nil))
  ([k default] (clojure.core/get-in @*hirop-session* k default)))

(defn remove! [k]
  (swap! *hirop-session* (fn [a b] (dissoc a b)) k))

(defn remove-keys! [ks]
  (swap! *hirop-session* (fn [a b] (apply dissoc a b)) ks))

(defn pop!
  ([k] (pop! k nil))
  ([k default]
     (if-let [v (get k default)]
       (do (remove! k)
           v)
       default)))

(defn clear! []
  (swap! *hirop-session* (constantly nil)))

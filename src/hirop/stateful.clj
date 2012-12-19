(ns hirop.stateful
  (:use [clojure.set :only [union]])
  (:require [hirop.core :as hirop]
            [hirop.backend :as backend]
            [hirop.session :as session]))

(defn init [doctypes contexts backend]
  (session/clear!)
  (session/put! :backend backend)
  (session/put! :doctypes doctypes)
  (session/put! :contexts contexts))

(defn- change-selection [context-id selection-id session-updater]
  (let [old-selection (get-in (session/get-in [:store context-id]) [:selections (keyword selection-id)])
        session (session/update! session-updater)
        new-selection (get-in session [context-id :store :selections (keyword selection-id)])
        changed-selection (set (map first (filter (fn [[k v]] (not= v (get old-selection k))) new-selection)))
        removed (set (map first (filter (fn [[k _]] (not (contains? new-selection k))) old-selection)))]
    (union changed-selection removed)))

(defn contexts []
  (session/get :contexts))

(defn doctypes []
  (session/get :doctypes))

(defn clean-contexts []
  (session/remove-keys! (keys (session/session-map))))

(defn clean-context [& {:keys [context-id]}]
  (session/remove-keys! [context-id]))

(defn create-context [context-name external-ids meta & {:keys [context-id]}]
  (let [contexts (session/get :contexts)
        doctypes (session/get :doctypes)
        context-name (keyword context-name)
        context (get contexts context-name)
        context (hirop/create-context context-name context doctypes external-ids)
        store (hirop/new-store context meta)]
    (when (get contexts context-name)
      (session/update!
       (fn [session]
         (assoc session context-id {:context context :store store}))))))

(defn new-document [doctype & {:keys [context-id]}]
  (let [{{context :context store :store} context-id} (session/update-in! [context-id :store] hirop/inc-uuid)
        new-id (hirop/get-uuid store)]
    (->
     (hirop/new-document context (keyword doctype))
     (hirop/assoc-hid new-id))))

(defn new-documents [doctype-map & {:keys [context-id]}]
  (let [document-map
        (reduce
         (fn [res [doctype n]]
           (assoc res doctype
                  (reduce
                   (fn [res _]
                     (let [{store :store context :context} (session/update-in! [context-id :store] hirop/inc-uuid)
                           new-id (hirop/get-uuid store)
                           document
                           (->
                            (hirop/new-document context (keyword doctype))
                            (hirop/assoc-hid new-id))]
                       (conj res document)))
                   []
                   (repeat n nil))))
         {}
         doctype-map)]
    document-map))

(defn get-document [id & {:keys [context-id]}]
  (hirop/get-document (session/get-in [context-id :store]) id))

(defn get-external-documents [& {:keys [context-id]}]
  (map (fn [[_ id]] (hirop/get-document (session/get-in [context-id :store]) id)) (session/get-in [context-id :context :external-ids])))

(defn get-configurations [& {:keys [context-id]}]
  (session/get-in [context-id :context :configurations]))

(defn get-configuration [doctype & {:keys [context-id]}]
  (let [doctype (keyword doctype)]
    (session/get-in [context-id :context :configurations doctype])))

(defn get-doctype [doctype & {:keys [context-id]}]
  (hirop/get-doctype (session/get-in [context-id :context]) (keyword doctype)))

(defn get-baseline [id & {:keys [context-id]}]
  (hirop/get-baseline (session/get-in [context-id :store]) id))

(defn commit [document & {:keys [context-id]}]
  (session/update-in! [context-id :store] hirop/commit document))

(defn mcommit [documents & {:keys [context-id]}]
  (session/update-in! [context-id :store] hirop/mcommit documents))

(defn pull [& {:keys [context-id]}]
  ;; TODO: here we should really query outside the transaction and then merge in the 
  ;; update (fetch might take a long time especially if db is remote)
  (session/update!
   (fn [session]
     (update-in session [context-id :store] hirop/pull (get-in session [context-id :context]) (partial backend/fetch (session/get :backend)))))
  {:result (if (hirop/any-conflicted? (session/get-in [context-id :store])) :conflict :success)})

(defn get-conflicted-ids [& {:keys [context-id]}]
  (hirop/get-conflicted-ids (session/get-in [context-id :store])))

(defn any-conflicted [& {:keys [context-id]}]
  (str (hirop/any-conflicted? (session/get-in [context-id :store]))))

(defn checkout-conflicted [& {:keys [context-id]}]
  (hirop/checkout-conflicted (session/get-in [context-id :store])))

(defn push [& {:keys [context-id]}]
  ;; TODO: see comments for pull, same thing. Save might take a long time (it might even throw an exception).
  (let [context (session/get-in [context-id :context])
        store (session/get-in [context-id :store])
        save-info (hirop/push-save store context (partial backend/save (session/get :backend)))]
    ;; With this version, save is not executed in an atom and only the data that has been saved is unstarred. 
    ;; Not sure what about re-starring, think about it (TODO).
    (session/update-in! [context-id :store] hirop/push-post-save save-info))
  ;; With this version, save is executed in atom swap!, which might be retried
  ;;(update-in-session! [:store] hirop/push backend/save)
  {:result (hirop/get-push-result (session/get-in [context-id :store]))})

(defn save [documents & {:keys [context-id]}]
  (let [context (session/get-in [context-id :context])
        store (session/update-in! [context-id :store] hirop/mcommit documents) 
        save-info (hirop/push-save store context (partial backend/save (session/get :backend)))]
    (session/update-in! [context-id :store] hirop/push-post-save save-info))
  {:result (hirop/get-push-result (session/get-in [context-id :store]))})

(defn history [id & {:keys [context-id]}]
  (backend/history (session/get :backend) id))

(defn checkout [& {:keys [doctype context-id]}]
  (if doctype
    (hirop/checkout (session/get-in [context-id :store]) doctype)
    (hirop/checkout (session/get-in [context-id :store]))))

(defn get-selected-ids [& {:keys [doctype selection-id context-id]}]
  (let [selection-id (or selection-id :default)]
    (if doctype
      (hirop/get-selected-ids (session/get-in [context-id :store]) (keyword selection-id) (keyword doctype))
      (hirop/get-selected-ids (session/get-in [context-id :store]) (keyword selection-id)))))

(defn checkout-selected [& {:keys [doctype selection-id context-id] :or {selection-id :default}}]
  (let [selection-id (or selection-id :default)]
    (if doctype
      (hirop/checkout-selected (session/get-in [context-id :store]) (keyword selection-id) (keyword doctype))
      (hirop/checkout-selected (session/get-in [context-id :store]) (keyword selection-id)))))

(defn select [& {:keys [selection-id id context-id]}]
  (let [selection-id (or selection-id :default)]
    (if id
      (change-selection
       context-id
       selection-id
       (fn [session]
         (update-in session [context-id :store] #(hirop/select-document % (get-in session [context-id :context]) id (keyword selection-id)))))
      (change-selection
       context-id
       selection-id
       (fn [session]
         (update-in session [context-id :store] #(hirop/select-defaults % (get-in session [context-id :context]) (keyword selection-id))))))))

(defn unselect [& {:keys [selection-id doctype context-id]}]
  (let [selection-id (or selection-id :default)]
    (change-selection
     context-id
     selection-id
     (fn [session]
       (update-in session [context-id :store] #(hirop/unselect % (get-in session [context-id :context]) (keyword selection-id) (keyword doctype)))))))


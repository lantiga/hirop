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

(defn- change-selection [selection-id session-updater]
  (let [old-selection (get-in (session/get :store) [:selections (keyword selection-id)])
        session (session/update! session-updater)
        new-selection (get-in session [:store :selections (keyword selection-id)])
        changed-selection (set (map first (filter (fn [[k v]] (not= v (get old-selection k))) new-selection)))
        removed (set (map first (filter (fn [[k _]] (not (contains? new-selection k))) old-selection)))]
    (union changed-selection removed)))

(defn contexts []
  (session/get :contexts))

(defn doctypes []
  (session/get :doctypes))

(defn clean-context []
  (session/remove-keys! [:store :context]))

(defn create-context [context-name external-ids meta]
  (let [contexts (session/get :contexts)
        doctypes (session/get :doctypes)
        loggedin (session/get :loggedin)
        context-name (keyword context-name)
        context (get contexts context-name)
        configurations (get-in contexts [context-name :configurations])
        context (hirop/create-context context-name context doctypes configurations external-ids)
        store (hirop/new-store context-name meta)]
    (when (get contexts context-name)
      (session/update!
       (fn [session]
         (-> session
             (assoc :context context)
             (assoc :store store)))))))

(defn new-document [doctype]
  (let [{context :context store :store} (session/update-in! [:store] hirop/inc-uuid)
        new-id (hirop/get-uuid store)
        document (hirop/new-document context (keyword doctype))
        document (assoc document :_id new-id :_rev (hirop/zero-rev))]
    document))

(defn new-documents [doctype-map]
  (let [document-map
        (reduce
         (fn [res [doctype n]]
           (assoc res doctype
                  (reduce
                   (fn [res _]
                     (let [{store :store context :context} (session/update-in! [:store] hirop/inc-uuid)
                           new-id (hirop/get-uuid store)
                           document (hirop/new-document context (keyword doctype))
                           document (assoc document :_id new-id :_rev (hirop/zero-rev))]
                       (conj res document)))
                   []
                   (repeat n nil))))
         {}
         doctype-map)]
    document-map))

(defn get-document [id]
  (hirop/get-document (session/get :store) id))

(defn get-external-documents []
  (map (fn [[_ id]] (hirop/get-document (session/get :store) id)) (session/get [:context :external-ids])))

(defn get-configurations []
  (session/get-in [:context :configurations]))

(defn get-configuration [doctype]
  (let [doctype (keyword doctype)]
    (session/get-in [:context :configurations doctype])))

(defn get-doctype [doctype]
  (hirop/get-doctype (session/get :context) (keyword doctype)))

(defn get-baseline [id]
  (hirop/get-baseline (session/get :store) id))

(defn commit [document]
  (session/update-in! [:store] hirop/commit document))

(defn mcommit [documents]
  (session/update-in! [:store] hirop/mcommit documents))

(defn pull []
  ;; TODO: here we should really query outside the transaction and then merge in the 
  ;; update (fetch might take a long time especially if db is remote)
  (session/update!
   (fn [session]
     (update-in session [:store] hirop/pull (:context session) (partial backend/fetch (session/get :backend)))))
  {:result (if (hirop/any-conflicted? (session/get :store)) :conflict :success)})

(defn get-conflicted-ids []
  (hirop/get-conflicted-ids (session/get :store)))

(defn any-conflicted []
  (str (hirop/any-conflicted? (session/get :store))))

(defn checkout-conflicted []
  (hirop/checkout-conflicted (session/get :store)))

(defn push []
  ;; TODO: see comments for pull, same thing. Save might take a long time (it might even throw an exception).
  (let [store (session/get :store)
        save-info (hirop/push-save store (partial backend/save (session/get :backend)))]
    ;; With this version, save is not executed in an atom and only the data that has been saved is unstarred. 
    ;; Not sure what about re-starring, think about it (TODO).
    (session/update-in! [:store] hirop/push-post-save save-info))
  ;; With this version, save is executed in atom swap!, which might be retried
  ;;(update-in-session! [:store] hirop/push backend/save)
  {:result (hirop/get-push-result (session/get :store))})

(defn save [documents]
  (let [store (session/update-in! [:store] hirop/mcommit documents) 
        save-info (hirop/push-save store (partial backend/save (session/get :backend)))]
    (session/update-in! [:store] hirop/push-post-save save-info))
  {:result (hirop/get-push-result (session/get :store))})

(defn history [id]
  (backend/history (session/get :backend) id))

(defn checkout
  ([]
     (hirop/checkout (session/get :store)))
  ([doctype]
     (hirop/checkout (session/get :store) doctype)))

(defn get-selected-ids
  ([selection-id]
     (hirop/get-selected-ids (session/get :store) (keyword selection-id)))
  ([selection-id doctype]
     (hirop/get-selected-ids (session/get :store) (keyword selection-id) (keyword doctype))))

(defn checkout-selected
  ([selection-id]
     (hirop/checkout-selected (session/get :store) (keyword selection-id)))
  ([selection-id doctype]
     (hirop/checkout-selected (session/get :store) (keyword selection-id) (keyword doctype))))

(defn select
  ([selection-id]
     (change-selection
      selection-id
      (fn [session]
        (update-in session [:store] #(hirop/select-defaults % (:context session) (keyword selection-id))))))
  ([selection-id id]
     (change-selection
      selection-id
      (fn [session]
        (update-in session [:store] #(hirop/select-document % (:context session) id (keyword selection-id)))))))

(defn unselect [selection-id doctype]
  (change-selection
   selection-id
   (fn [session]
     (update-in session [:store] #(hirop/unselect % (:context session) (keyword selection-id) (keyword doctype))))))


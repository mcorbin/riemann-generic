(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :refer :all]
            [riemann-generic.output :as output]
            [clojure.tools.logging :refer :all]))

(defn threshold
  [{:keys [name params output] :as opts}]
  (where (and (service (:service params))
              ((:operation params) (:threshold params)))
    (with :state "critical"
      (apply sdo (flatten output)))))

(defn threshold-throttle
  [{:keys [name params output] :as opts}]
  (where (and (service (:service params))
           ((:operation params) (:threshold params)))
    (throttle (:nb-events params) (:duration params)
      (with :state "critical"
        (apply sdo (flatten output))))))

(defn above-dt
  [{:keys [name params output] :as opts}]
  (above (:threshold params) (:duration params)
    (with :state "critical"
      (apply sdo (flatten output)))))

(defn below-dt
  [{:keys [name params output] :as opts}]
  (below (:threshold params) (:duration params)
    (with :state "critical"
      (apply sdo (flatten output)))))

(defn outside-dt
  [{:keys [name params output] :as opts}]
  (outside (:min-threshold params) (:max-threshold params) (:duration params)
    (with :state "critical"
      (apply sdo (flatten output)))))

(defn between-dt
  [{:keys [name params output] :as opts}]
  (between (:min-threshold params) (:max-threshold params) (:duration params)
    (with :state "critical"
      (apply sdo (flatten output)))))

(defn critical-dt
  [{:keys [name params output] :as opts}]
  (critical (:duration params)
    (with :state "critical"
      (apply sdo (flatten output)))))

;; {:streams [{:name "foo"
;;             :stream "critical-dt"
;;             :params {:duration 100}
;;             :outputs ["slack-foo" "email bar"]
;;             } ]
;;  :outputs [{:name "slack-foo"
;;             :stream "slack-alert"
;;             :params {:credentials {}
;;                      :options {}}}]}


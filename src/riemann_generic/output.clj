(ns riemann-generic.output
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann.slack :as slack]
            [riemann.pagerduty :as pagerduty]
            [riemann.email :as email]
            [clojure.tools.logging :refer :all]))

(defn slack-alert
  [{:keys [name params]}]
  (slack/slack (:credentials params) (:options params)))


(defn pagerduty-alert
  [{:keys [name params]}]
  (:trigger (pagerduty/pagerduty (:service-key params)
                                 (:options params))))

(defn email-alert
  [{:keys [name params]}]
  ((email/mailer {:options params}) (:address params)))




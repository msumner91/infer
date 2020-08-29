(ns infer.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.math.numeric-tower :as nmath]
            [oz.core :as oz]
            [java-time :as jt])
  (:import org.apache.commons.io.input.BOMInputStream)
  (:gen-class))

(defn- csv->map
  "Converts csv data from seq of vec to seq of maps"
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map keyword)
            (repeat))
       (rest csv-data)))

(defn- get-cols
  "Selects cols ks from data"
  [ks data]
  (map #(select-keys % ks) data))

(defn- add-column
  "Add column to data with col-key -> col-val"
  [data col-key col-val]
  (map #(assoc % col-key col-val) data))

(defn- agg-by-eod
  "Aggregates data by :DATETIME (keeping last entry only) :DATETIME must be formatted as a LocalDate"
  [data]
  (->> (group-by :DATETIME data)
       (vals)
       (map last)
       (sort-by :DATETIME)))

(defn- replace-col
  "Replaces column k in data with values from newvals"
  [data k new-data]
  (map (fn [[m v]] (assoc m k v))
       (partition 2 (interleave data new-data))))

(defn do-read
  "Reads csv file to vec of maps cleaning if necessary"
  [filename]
  (with-open [reader (-> filename
                         io/input-stream
                         BOMInputStream.
                         io/reader)]
    (->> (csv/read-csv reader)
         (csv->map)
         (into []))))

(defn ewma
  "Compute EWMA for seq of prices over period p"
  [pr p]
  (let [d (/ 2 (inc p))
        d-c (- 1 d)
        init (first pr)]
    (if (some? init)
      (loop [prT (rest pr)
             previous init
             result [init]]
        (if-let [c (first prT)]
          (let [emaC (+ (* d c) (* d-c previous))]
            (recur (rest prT) emaC (conj result emaC)))
          result))
      (throw (Exception. "Need at least one price")))))

(defn -main [file period-f period-s f-scalar step lim]
  (let [eod-readings (->> (do-read file)
                          (get-cols [:DATETIME :PRICE])
                          (filter #(and (seq (:DATETIME %)) (seq (:PRICE %))))
                          (map (fn [m] (-> (update m :DATETIME #(jt/local-date "yyyy-MM-dd HH:mm:ss" %))
                                           (update :PRICE #(Double/parseDouble %)))))
                          (agg-by-eod))

        ;; Take sample if specified, extract price values as seq
        data (if (some? lim) (take lim eod-readings) eod-readings)
        price-s (vec (map :PRICE data))

        ;; Compute returns ewma (percent and price)
        pct-sq-rets (map (fn [[x y]] (nmath/expt (/ (- y x) x) 2)) (partition 2 1 price-s))
        ewma-pct-rets (map #(nmath/sqrt %) (ewma pct-sq-rets 36))
        price-vol (->> (interleave price-s ewma-pct-rets)
                       (partition 2)
                       (map (fn [[p ema]] (* p ema))))

        ;; Compute price ewma fast/slow
        ewma-f (ewma price-s period-f)
        ewma-s (ewma price-s period-s)

        ;; Compute vol adjusted ewma cross & forecast
        ewma-cross (map (fn [[f s]] (- f s)) (partition 2 (interleave ewma-f ewma-s)))
        ewma-vol-adj (map (fn [[cross vol]] (/ cross vol)) (partition 2 (interleave ewma-cross price-vol)))
        avg-abs (/ (reduce + (map #(nmath/abs %) ewma-vol-adj)) (count ewma-vol-adj))
        _ (printf "Suggested scalar: %s\n" (/ 10 avg-abs))
        forecast (vec (map #(min 20 (max -20 (* f-scalar %))) ewma-vol-adj))

        ;; Merge results
        data-str-d (map #(update % :DATETIME str) data) ;; format date back as string for visualisation
        data-ewma-f (-> (replace-col data-str-d :PRICE ewma-f)
                        (add-column :CATEGORY "EWMA Fast"))
        data-ewma-s (-> (replace-col data-str-d :PRICE ewma-s)
                        (add-column :CATEGORY "EWMA Slow"))
        data-forecast (-> (replace-col data-str-d :PRICE forecast)
                          (add-column :CATEGORY "Forecast (w/o multiplier)"))
        data-prices (add-column data-str-d :CATEGORY "Price")]

    (oz/view! {:vconcat [{:title (format "EWMA f/s %s/%s" period-f period-s)
                          :mark "line"
                          :data {:values (concat data-prices data-ewma-f data-ewma-s)}
                          :encoding {:x {:timeUnit {:unit "yearmonthdate" :step step} :field "DATETIME" :type "ordinal"}
                                     :y {:field "PRICE" :type "quantitative"}
                                     :color {:field "CATEGORY" :type "nominal"}}}

                         {:title (format "Rule forecasts")
                          :data {:values data-forecast}
                          :layer [{:mark {:type "line"
                                          :point {:filled false
                                                  :fill "white"}}
                                   :encoding {:x {:timeUnit {:unit "yearmonthdate" :step step} :field "DATETIME" :type "ordinal"}
                                              :y {:field "PRICE" :type "quantitative"}
                                              :color {:field "CATEGORY" :type "nominal"}}}

                                  {:mark "rule"
                                   :encoding {:y {:value 100}}}]}]})))


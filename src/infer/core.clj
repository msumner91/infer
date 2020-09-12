(ns infer.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.math.numeric-tower :as nmath]
            [oz.core :as oz]
            [java-time :as jt])
  (:import org.apache.commons.io.input.BOMInputStream)
  (:gen-class))

(def BUSINESS_DAYS_IN_YEAR 256.0)
(def ROOT_BDAYS_INYEAR (nmath/sqrt BUSINESS_DAYS_IN_YEAR))
(def ARBITRARY_FORECAST_CAPITAL 10000000.0)
(def DEFAULT_ANN_RISK_TARGET 0.16)
(def DEFAULT_AVG_ABS_FORECAST 10.0)
(def DEFAULT_COST {:SLIPPAGE 0.0 :BLOCKCOM 0.0 :PCTCOST 0.0 :PTRADECOM 0.0})
(def DEFAULT_BLOCK_VALUE 100.0)
(def DEFAULT_PRICE_POINT_VALUE 1.0)

(defn- avg [coll] (->> (count coll) (/ (reduce + coll))))

(defn- avg-by-key [k data] (let [sz (count data)
                                 vs (map k data)
                                 total (reduce + vs)]
                             (/ total sz)))

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

(defn- replace-col
  "Replaces column k in data with values from newvals"
  [data k new-data]
  (map (fn [[m v]] (assoc m k v))
       (partition 2 (interleave data new-data))))

(defn- agg-by-eod
  "Aggregates data by :DATETIME (keeping last entry only if more than one). :DATETIME must be formatted as a LocalDate"
  [data]
  (->> (group-by :DATETIME data)
       (vals)
       (map last)
       (sort-by :DATETIME)))

(defn- agg-by-dt
  "Aggregates data by :DATETIME with frequency determined by f"
  [data f]
  (let [agg-data (group-by #(->> (:DATETIME %) f) data)
        agg-data-avg (map (fn [[k v]] {:DATETIME k :PRICE (avg-by-key :PRICE v)}) (seq agg-data))]
    agg-data-avg))

(defn- turnover
  "Calculate average turnover for instrument forecast. Used to scale price/vol costs"
  [forecast-s]
  (let [norm-forecast (map #(/ % DEFAULT_AVG_ABS_FORECAST) forecast-s)
        abs-daily-diff (map (fn [[x y]] (nmath/abs (- y x))) (partition 2 1 norm-forecast))
        avg-daily-diff (avg abs-daily-diff)]
    (* avg-daily-diff BUSINESS_DAYS_IN_YEAR)))

(defn- get-forecast-scaled-sr-cost
  "Calculates annualised cost for an instrument using SR of price sampled from the past year and forecast turnover.
   
   Intuition: 
     - More volatile forecast changes (up or down) scales whatever the cost is (since it factors into the magnitude of the trade) 
     - Trivially higher price => higher cost
     - More volatile price changes should offset cost of trading this instrument (more opportunity for profit making)
     - Less volatile price changes should magnify cost of trading this instrument (less opportunity for profit making so we are left with price as dominant term)"
  [price-s returns-vol forecast-s]
  (let [forecast-turnover (turnover forecast-s)
        _ (println "Forecast turnover: " forecast-turnover)
        price-slip (:SLIPPAGE DEFAULT_COST)
        block-com (:BLOCKCOM DEFAULT_COST)
        pct-cost (:PCTCOST DEFAULT_COST)
        per-trade-com (:PTRADECOM DEFAULT_COST)
        last-date (:DATETIME (peek price-s)) ;; note price-s is a vec, so it is last not first date
        start-date (jt/minus last-date (jt/years 1))

        price-subset (drop-while #(jt/before? (:DATETIME %) start-date) price-s)
        vol-subset (drop-while #(jt/before? (:DATETIME %) start-date) returns-vol)

        avg-price-subset (avg (map #(:PRICE %) price-subset))
        avg-vol-subset (avg (map #(:PRICE %) vol-subset))

        price-block-comm (/ block-com DEFAULT_BLOCK_VALUE)
        price-pct-cost (* avg-price-subset pct-cost)
        _ (println "Avg price subset: " avg-price-subset)
        price-per-trade-cost (/ per-trade-com DEFAULT_BLOCK_VALUE)
        price-total (+ price-slip price-block-comm price-pct-cost price-per-trade-cost)
        avg-annual-vol (* avg-vol-subset ROOT_BDAYS_INYEAR)
        _ (println "Price total: " price-total)
        _ (println "Avg-ann-vol: " avg-annual-vol)
        _ (println "Product" (* 2.0 (/ price-total avg-annual-vol)))
        sr-cost (* 2.0 (/ price-total avg-annual-vol))]
    (* forecast-turnover sr-cost)))

(defn- calc-risk-capital
  "Calculate annualised and daily risk capital based on targets"
  [price-s capital ann-risk-target]
  (let [capital-ts (repeat (count price-s) capital)
        daily-risk-capital (map #(* % (/ ann-risk-target ROOT_BDAYS_INYEAR)) capital-ts)
        ann-risk (map #(* % ann-risk-target) capital-ts)]
    (vector ann-risk daily-risk-capital)))

(defn- calc-pnl
  "Calculate daily PnL for instrument and forecast/trading rule. 
   Does not factor in cost of trading which is applied to the result."
  [price-s
   forecast-s
   daily-risk-capital-s
   returns-vol-s
   price-point-val]
  (let [fx-s (repeat (count price-s) 1.0)
        adj-daily-risk-cap (map #(/ (* % 1.0 1.0) 10) daily-risk-capital-s)
        forecast-capital (map #(* % %2) forecast-s adj-daily-risk-cap)
        scaled-daily-vol (map #(* price-point-val % %2) returns-vol-s fx-s)

        scaled-positions (map #(/ % %2) forecast-capital scaled-daily-vol) ; size positions based on forecast signal, daily capital & returns vol
        daily-trade-signal (map (fn [[x y]] (- y x)) (partition 2 1 scaled-positions))
        price-rets (map (fn [[x y]] (- y x)) (partition 2 1 price-s))
        i-ccy-ret (reductions + (map #(* % %2 price-point-val) scaled-positions price-rets))
        b-ccy-ret (map #(* % %2) i-ccy-ret fx-s)]
    (vector scaled-positions daily-trade-signal i-ccy-ret b-ccy-ret fx-s)))

(defn- calc-annualised-costs
  "Calculate annualised costs in base currency for instrument from annualised sr-cost and annualised risk capital"
  [sr-cost ann-risk use-fx]
  (let [cost-i-ccy (reductions + (map #(/ (* sr-cost -1 %) BUSINESS_DAYS_IN_YEAR) ann-risk))
        cost-i-ccy-diff (map (fn [[x y]] (- y x)) (partition 2 1 cost-i-ccy))
        cost-b-ccy (map #(* % %2) cost-i-ccy-diff use-fx)]
    (vector cost-i-ccy-diff cost-b-ccy)))

(defn- with-str-d [data] (map #(update % :DATETIME str) data))

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

(defn -main [file period-f period-s f-scalar & [t-unit step lim]]
  (let [eod-readings (->> (do-read file)
                          (get-cols [:DATETIME :PRICE])
                          (filter #(and (seq (:DATETIME %)) (seq (:PRICE %))))
                          (map (fn [m] (-> (update m :DATETIME
                                                   #(try (jt/local-date "yyyy-MM-dd" %)
                                                         (catch Exception _
                                                           (jt/local-date "yyyy-MM-dd HH:mm:ss" %))))
                                           (update :PRICE #(Double/parseDouble %)))))
                          (agg-by-eod))

        ; Take sample if specified, extract price values as seq
        time-unit (if (some? t-unit) t-unit "year")
        freq (cond
               (= time-unit "year") #(jt/year %)
               (= time-unit "yearmonth") #(jt/year-month %)
               :else #(jt/local-date %))
        data (if (some? lim) (take lim eod-readings) eod-readings)
        price-s (vec (map :PRICE data))

        ; Compute returns volatility
        returns-pct-var (map (fn [[x y]] (nmath/expt (/ (- y x) x) 2)) (partition 2 1 price-s))
        returns-pct-vol (map #(nmath/sqrt %) (ewma returns-pct-var 36))
        returns-vol (map #(* % %2) price-s returns-pct-vol)

        ; Compute vol adjusted ewma cross & forecast
        ewma-f (ewma price-s period-f)
        ewma-s (ewma price-s period-s)
        ewma-cross (map #(- % %2) ewma-f ewma-s)
        ewma-vol-adj (map #(/ % %2) ewma-cross returns-vol)
        avg-abs (/ (reduce + (map #(nmath/abs %) ewma-vol-adj)) (count ewma-vol-adj))
        _ (printf "Suggested scalar: %s\n" (/ 10 avg-abs))
        forecast (vec (map #(min 20 (max -20 (* f-scalar %))) ewma-vol-adj))

        sr-cost (get-forecast-scaled-sr-cost (vec data) (vec (replace-col data :PRICE returns-vol)) forecast)
        _ (println "SrCost: " sr-cost)

        [ann-risk daily-risk-capital] (calc-risk-capital price-s ARBITRARY_FORECAST_CAPITAL DEFAULT_ANN_RISK_TARGET)
        _ (println "Daily risk: " (first daily-risk-capital))
        _ (println "Ann risk: " (first ann-risk))
        [_, _, _, b-ccy-ret, use-fx] (calc-pnl price-s forecast daily-risk-capital returns-vol DEFAULT_PRICE_POINT_VALUE)
        [_ cost-b-ccy] (calc-annualised-costs sr-cost ann-risk use-fx)
        daily-ret-base-ccy (map #(+ % %2) b-ccy-ret cost-b-ccy)

        ; Merge results
        data-ewma-f (-> (replace-col data :PRICE ewma-f)
                        (agg-by-dt freq)
                        (add-column :CATEGORY "EWMA Fast")
                        (with-str-d))
        data-ewma-s (-> (replace-col data :PRICE ewma-s)
                        (agg-by-dt freq)
                        (add-column :CATEGORY "EWMA Slow")
                        (with-str-d))
        data-forecast (-> (replace-col data :PRICE forecast)
                          (agg-by-dt freq)
                          (add-column :CATEGORY "Forecast")
                          (with-str-d))
        data-forecast-ret (-> (replace-col data :PRICE daily-ret-base-ccy)
                              (agg-by-dt freq)
                              (add-column :CATEGORY "Forecast returns daily")
                              (with-str-d))
        data-prices (-> data (agg-by-dt freq) (add-column :CATEGORY "Price") (with-str-d))]

    (oz/view! {:vconcat [{:title (format "EWMA f/s %s/%s/%s" period-f period-s (-> (io/file file) (.getName)))
                          :mark {:type "line"
                                 :interpolate "monotone"}
                          :data {:values (concat data-prices data-ewma-f data-ewma-s)}
                          :encoding {:x {:timeUnit {:unit time-unit :step step} :field "DATETIME" :type "ordinal"}
                                     :y {:field "PRICE" :type "quantitative" :axis {:title "Price ($)"}}
                                     :color {:field "CATEGORY" :type "nominal"}}}

                         {:title (format "Trading rule forecast")
                          :data {:values data-forecast}
                          :layer [{:mark {:type "line"
                                          :interpolate "monotone"}
                                   :encoding {:x {:timeUnit {:unit time-unit :step step} :field "DATETIME" :type "ordinal"}
                                              :y {:field "PRICE" :type "quantitative" :axis {:title "Trading rule forecast"}}
                                              :color {:field "CATEGORY" :type "nominal"}}}]}

                         {:title (format "PnL")
                          :data {:values data-forecast-ret}
                          :layer [{:mark {:type "line"
                                          :point {:filled false
                                                  :fill "white"}
                                          :interpolate "monotone"}
                                   :encoding {:x {:timeUnit {:unit time-unit :step step} :field "DATETIME" :type "ordinal"}
                                              :y {:field "PRICE" :type "quantitative" :axis {:title "PnL"}}
                                              :color {:field "CATEGORY" :type "nominal"}}}]}]})
    '1))


# Load libraries
library(fable)
library(fabletools)
library(tsibble)
library(readr)
library(dplyr)
library(lubridate)
library(ggplot2)
library(feasts)
library(fable.prophet)

# 1. Load cleaned data
fc_data <- read_csv("cleaned_monthly_listening_data.csv")

# 2. Convert to tsibble
fc_data_ts <- fc_data %>%
  mutate(MONTH = yearmonth(MONTH)) %>%
  as_tsibble(index = MONTH)

# 3. Visual Check
ggplot(fc_data_ts, aes(x = MONTH, y = TOTAL_HOURS)) +
  geom_line(color = "blue") +
  labs(title = "Monthly Listening Trend", x = "Month", y = "Hours Listened")

# 4. Fit All Major Models
fc_models <- fc_data_ts %>%
  model(
    ets_model = ETS(TOTAL_HOURS),
    arima_model = ARIMA(TOTAL_HOURS),
    sarima_model = ARIMA(TOTAL_HOURS ~ pdq(1,1,1) + PDQ(1,1,1)), # Manual seasonal ARIMA
    nn_model = NNETAR(TOTAL_HOURS),
    prophet_model = prophet(TOTAL_HOURS ~ season(period = "1 year"))
  )

# 5. Check Accuracy
accuracy_table <- fc_models %>%
  accuracy()

cat("\n--- Model Accuracy Table ---\n")
print(accuracy_table)

# 6. Choose best model based on lowest RMSE
# (Pick after seeing output)

# 7. Example: Select Best Model (e.g., ARIMA)
fc_best_model <- fc_models %>%
  select(nn_model)   # replace 'arima_model' with actual best one after you see

# 8. Forecast 12 Months Ahead
fc_forecast_best <- fc_best_model %>%
  forecast(h = "12 months")

# 9. Save the Forecast
fc_forecast_best_tbl <- fc_forecast_best %>%
  as_tibble()

write_csv(fc_forecast_best_tbl, "final_forecasted_listening_hours.csv")


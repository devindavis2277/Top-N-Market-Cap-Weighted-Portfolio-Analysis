library(dplyr)
library(haven)
library(lubridate)
library(data.table)   
library(ggplot2)


# read and basic prep

mret <- read_sas("~/Desktop/WVS/mret7023.sas7bdat") %>% as.data.frame()

# keep common stocks
mret <- mret %>% filter(SHRCD %in% c(10,11))

# replace missing DIVAMT with 0
mret <- mret %>% mutate(DIVAMT = coalesce(DIVAMT, 0))

# ensure DATE is Date class
if(!inherits(mret$DATE, "Date")) {
  # try lubridate parsing if it's numeric or char
  mret$DATE <- as.Date(mret$DATE)
}


# adjust prices/dividends/shares and compute mcap

mret <- mret %>%
  arrange(PERMNO, DATE) %>%
  group_by(PERMNO) %>%
  mutate(
    year  = year(DATE),
    month = month(DATE),
    
    # adjust price/dividend/shares using CFACPR / CFACSHR
    PRC_adj    = abs(PRC) / CFACPR,
    DIVAMT_adj = DIVAMT / CFACPR,
    SHROUT_adj = SHROUT * CFACSHR,
    
    # market cap measured at current observation (adjusted)
    mcap = PRC_adj * SHROUT_adj,
    
    # previous-month market cap (lag)
    mcap_lag = lag(mcap, 1),
    
    # construct adjusted return based on adjusted prices & dividends
    PRC_adj_lag = lag(PRC_adj, 1),
    myRET = (PRC_adj - PRC_adj_lag + DIVAMT_adj) / PRC_adj_lag
  ) %>%
  ungroup()

# drop rows where previous-month mcap is missing (can't rank)
mret <- mret %>% filter(!is.na(mcap_lag))


# largest firm at end of previous month
# for each DATE, pick stock with largest mcap_lag

# create table of perch-month top firm
topfirm_by_date <- mret %>%
  group_by(DATE) %>%
  # choose the PERMNO with max previous-month mcap (mcap_lag)
  filter(mcap_lag == max(mcap_lag, na.rm = TRUE)) %>%
  # if ties, keep first (should be rare)
  slice(1) %>%
  ungroup() %>%
  select(DATE, PERMNO, COMNAM, mcap_lag) %>%
  rename(mcap_prev = mcap_lag,
         company = COMNAM)

# save a csv of top firm by date
write.csv(topfirm_by_date,
          file = "~/Desktop/Assignment_4/Devin_Davis_topfirm_by_date.csv",
          row.names = FALSE)

# compute which company held #1 for the longest consecutive stretch (in months)
# we need to detect consecutive runs of the same PERMNO in the time-ordered top list

# ensure sorted by DATE
topfirm_by_date <- topfirm_by_date %>% arrange(DATE)

# use data.table rleid approach for runs
dt_top <- as.data.table(topfirm_by_date)
dt_top[, run_id := rleid(PERMNO)]
run_lengths <- dt_top[, .(start_date = min(DATE),
                          end_date   = max(DATE),
                          months = .N,
                          PERMNO = PERMNO[1],
                          company = company[1]),
                      by = run_id] %>%
  arrange(-months)

# longest run:
longest_run <- run_lengths[1, ]

# save run table
write.csv(run_lengths,
          file = "~/Desktop/Assignment_4/Devin_Davis_topfirm_runs.csv",
          row.names = FALSE)

# print result to console
message("Company with loAssngest consecutive #1 position:")
print(longest_run)


# build 10 dynamic VW portfolios (top 10,20,...,100 by mcap_lag)
# and compute monthly VW returns for each portfolio.
# use the myRET at current DATE and weights = mcap_lag (previous-month cap)

# restrict analysis window to 1970-01-01 through 2023-12-31
mret <- mret %>% filter(year >= 1970 & year <= 2023)

# for each DATE, rank by mcap_lag and create flags for being in topN
# we'll create a long format with portfolio label (top10, top20, ...)
top_ns <- seq(10, 100, by = 10)

# compute ranks per DATE
mret <- mret %>%
  group_by(DATE) %>%
  mutate(rank_by_mcap = dense_rank(desc(mcap_lag))) %>%
  ungroup()

# for each top N compute VW portfolio return using weights = mcap_lag
# we'll build a data.frame with DATE and VW returns for each N
portfolio_returns <- lapply(top_ns, function(N) {
  tmp <- mret %>%
    filter(rank_by_mcap <= N) %>%
    group_by(DATE) %>%
    summarise(
      N_stocks = n(),
      VWRET_topN = ifelse(sum(!is.na(myRET))==0, NA,
                          sum(myRET * mcap_lag, na.rm = TRUE) / sum(mcap_lag[!is.na(myRET)], na.rm = TRUE))
    ) %>%
    ungroup() %>%
    mutate(topN = N)
  tmp
})

portfolio_returns_df <- bind_rows(portfolio_returns) %>%
  tidyr::pivot_wider(names_from = topN, values_from = VWRET_topN,
                     names_prefix = "VW_top") %>%
  arrange(DATE)

# for convenience also create a long form (DATE, topN, VWRET)
portfolio_returns_long <- bind_rows(lapply(top_ns, function(N) {
  df <- portfolio_returns[[which(top_ns==N)]]
  df %>% select(DATE, VWRET_topN) %>% rename(VWRET = VWRET_topN) %>% mutate(topN = N)
}))

# save portfolio returns CSV
write.csv(portfolio_returns_long,
          file = "~/Desktop/Assignment_4/Devin_Davis_topN_portfolio_returns_long.csv",
          row.names = FALSE)

# correlations of these portfolios with CRSP VWRETD
# over 1970-2023 (we already filtered)

# need CRSP VWRETD by DATE; mret1 earlier computed VWRETD per DATE in your code
# recompute VWRETD from mret (use CRSP's VWRETD from existing variable if present)
# we'll take VWRETD from any row for that DATE (they should be identical across PERMNO)
vwretd_by_date <- mret %>%
  group_by(DATE) %>%
  summarise(VWRETD = first(na.omit(VWRETD))) %>% # picks CRSP's VWRETD if not NA
  ungroup()

# merge VWRETD with each portfolio's returns (long)
port_vs_index <- portfolio_returns_long %>%
  left_join(vwretd_by_date, by = "DATE")

# compute correlations for each topN
corrs <- port_vs_index %>%
  group_by(topN) %>%
  summarise(
    valid_obs = sum(!is.na(VWRET) & !is.na(VWRETD)),
    correlation = ifelse(valid_obs > 2, cor(VWRET, VWRETD, use = "pairwise.complete.obs"), NA_real_)
  ) %>%
  arrange(topN)

print(corrs)

# save correlations
write.csv(corrs,
          file = "~/Desktop/Assignment_4/Devin_Davis_topN_correlations_with_VWRETD.csv",
          row.names = FALSE)


# visualize the 10 correlation coefficients

# simple barplot / histogram of correlations
p_corr <- ggplot(corrs, aes(x = factor(topN), y = correlation)) +
  geom_col() +
  labs(title = "Correlation of Top-N VW Portfolio Returns with VWRETD (1970-2023)",
       x = "Top N (largest firms by previous-month mcap)",
       y = "Correlation with VWRETD") +
  theme_minimal()

ggsave(filename = "~/Desktop/Assignment_4/Devin_Davis_topN_correlations_barplot.png",
       plot = p_corr, width = 8, height = 5)

# also histogram view (though 10 points)
p_hist <- ggplot(corrs, aes(x = correlation)) +
  geom_histogram(bins = 10) +
  labs(title = "Histogram of correlations (Top-N vs VWRETD)",
       x = "Correlation", y = "Count") +
  theme_minimal()

ggsave(filename = "~/Desktop/Assignment_4/Devin_Davis_topN_correlations_hist.png",
       plot = p_hist, width = 6, height = 4)


# quick interpretation (printed)

message("\nSummary of main outputs (saved to Desktop):")
message(" - Devin_Davis_topfirm_by_date.csv                         (largest firm each date)")
message(" - Devin_Davis_topfirm_runs.csv                            (consecutive runs & lengths)")
message(" - Devin_Davis_topN_portfolio_returns_long.csv             (VW returns for topN portfolios)")
message(" - Devin_Davis_topN_correlations_with_VWRETD.csv           (correlations table)")
message(" - Devin_Davis_topN_correlations_barplot.png               (barplot of correlations)")
message(" - Devin_Davis_topN_correlations_hist.png                  (histogram)\n")

message("Printing correlations table:")
print(corrs)

message("\nInterpretation hints:")
message(" - Expect correlations close to 1 for large top-N portfolios (they behave much like market index).")
message(" - Smaller topN (like top10) may deviate more variability-wise; increasing N increases coverage of market cap and typically raises correlation to VWRETD.")
message(" - Low correlation could signal that top-N concentrated stocks have idiosyncratic performance relative to the market index.\n")

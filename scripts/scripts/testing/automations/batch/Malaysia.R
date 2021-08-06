def main():
  
  df <- fread("https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv") %>%
    mutate(`Daily change in cumulative total` = .[[2]] + .[[3]]) %>%
    subset(., select=-c(`pcr`, `rtk-ag`)) %>%
    mutate(`Cumulative total`=cumsum(`Daily change in cumulative total`))
  
  setnames(df, "date", "Date")
  
  df[, Country := "Malaysia"]
  df[, `Source URL` := "https://github.com/MoH-Malaysia/covid19-public"]
  df[, `Source label` := "Malaysia Ministry of Health"]
  df[, Notes := "Made available by the Malaysia Ministry of Health on GitHub"]
  
if __name__ == "__main__":
  main()

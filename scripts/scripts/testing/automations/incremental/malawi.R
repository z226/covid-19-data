url <- "https://www.malawipublichealth.org/"

count <- read_html(url) %>%
    html_nodes('.elementor-counter-number') %>%
    html_attr('data-to-value') %>%
    magrittr::extract(7) %>%
    as.integer()

date <- read_html(url) %>%
    html_nodes('elementor-heading-title .elementor-size-default') %>% 
    html_text()

add_snapshot(
    count = count,
    sheet_name = "Malawi",
    country = "Malawi",
    units = "samples tested",
    source_url = url,
    source_label = "Public Health Institute of Malawi",
)

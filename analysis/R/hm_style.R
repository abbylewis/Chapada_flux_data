color.vals=c("#F29559", "#F2B576","#F2D492", "#C1BE7E","#888F5C", "#4E603A", "#1F3829")
color.gradient <- colorRampPalette(color.vals)(9)

# Theme for all plots
theme_hm <- egg::theme_article() +
  theme(
    panel.grid.major = element_line(color = "grey93", linewidth = 0.5),
    panel.grid.minor = element_line(color = "grey95", linewidth = 0.25),
    legend.position = "bottom",
    legend.title.position = "top",
    plot.background = element_rect(fill = "white"),
    legend.text = element_text(size = 8),
    legend.key.spacing.y = unit(0, "cm"),
    legend.key.spacing.x = unit(0.5, "cm"),
    legend.justification = "left",
    legend.location = "plot"
  )

# Themes for Fig 2
panel_theme <- theme(
  plot.margin = ggplot2::margin(0.1, 1, 0.1, 1, unit = "mm")
)
top_theme <- theme(
  axis.text.x = element_blank(),
  axis.ticks.x = element_blank()
)

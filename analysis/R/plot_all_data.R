make_annual_plot <- function(
    data,
    x,
    y,
    colour = NULL,
    ylabel,
    line_colour = "grey20",
    zero_line = "grey80",
    annotate = T
) {
  
  p <- ggplot(data, aes(.data[[x]], .data[[y]]))
  
  if (annotate) {
    p <- p +
      annotate(
        "rect",
        xmin = start_date,
        xmax = end_date + days(1),
        ymin = -Inf,
        ymax = Inf,
        fill = "grey50",
        alpha = 0.2
      )
  }
  
  p <- p +
    coord_cartesian(xlim = annual_xlim) +
    scale_x_date(
      date_breaks = "1 month",
      date_labels = "%b"
    ) +
    theme_hm +
    theme(
      axis.title.x = element_blank()
    ) +
    labs(y = ylabel)
  
  if (!is.null(zero_line)){
    p <- p +
      geom_hline(
        yintercept = 0,
        color = zero_line
      )
  }
  
  if (is.null(colour)) {
    p <- p +
      geom_line(
        linewidth = 0.5,
        color = line_colour
      )
  } else {
    p <- p +
      geom_line(
        aes(color = .data[[colour]]),
        linewidth = 0.5
      ) +
      scale_color_manual(
        values = color.gradient,
        name = colour
      )
  }
  
  p
}


make_focal_plot <- function(
    data,
    x,
    y,
    colour = NULL,
    ylabel,
    line_colour = "grey20",
    zero_line = "grey80",
    background = F
) {
  
  p <- ggplot(data, aes(.data[[x]], .data[[y]])) +
    scale_x_datetime(
      date_breaks = "3 days",
      date_labels = "%d %b"
    ) +
    theme_hm +
    theme(
      axis.title.x = element_blank(),
      axis.title.y = element_blank()
    ) +
    labs(y = ylabel)
  
    if(background){
      p <- p + 
        annotate(
          "rect",
          xmin = start_datetime,
          xmax = end_datetime,
          ymin = -Inf,
          ymax = Inf,
          fill = "grey50",
          alpha = 0.2
        )
    }
    
  if (!is.null(zero_line)){
    p <- p +
      geom_hline(
        yintercept = 0,
        color = zero_line
      )
  }
  
  if (is.null(colour)) {
    p <- p +
      geom_line(
        linewidth = 0.5,
        color = line_colour
      )
  } else {
    p <- p +
      geom_line(
        aes(color = .data[[colour]]),
        linewidth = 0.5
      ) +
      scale_color_manual(
        values = color.gradient,
        name = "Chamber"
      )
  }
  
  p 
}

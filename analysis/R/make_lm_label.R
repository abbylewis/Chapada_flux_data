#' Make lm stat labels for plots
#'
#' @param data Dataset to use
#' @param xvar X-axis variable
#' @param yvar Y-axis variable
#' @param group_var Grouping variable 1 (if applicable)
#' @param group_var2 Grouping variable 2 (if applicable)
#'
#' @returns Data frame with stats and labels for plot

make_lm_label <- function(data, xvar, yvar, group_var = NULL, group_var2 = NULL) {
  group_vars <- Filter(Negate(is.null), list(group_var, group_var2))

  # Number of comparisons (for Bonferroni correction)
  n_comp <- if (length(group_vars) > 0) {
    prod(sapply(group_vars, function(g) length(unique(data[[g]]))))
  } else {
    1
  }

  data %>%
    {
      if (length(group_vars) > 0) {
        dplyr::group_by(., !!!rlang::syms(group_vars))
      } else {
        .
      }
    } %>%
    dplyr::summarise(
      model = list(lm(reformulate(xvar, yvar), data = cur_data())),
      intercept = coef(model[[1]])[1],
      slope = coef(model[[1]])[2],
      r2 = summary(model[[1]])$r.squared,
      p = summary(model[[1]])$coefficients[2, 4],
      p_adj = p.adjust(p, n = n_comp, method = "bonferroni"),
      x_pos = min(.data[[xvar]], na.rm = TRUE),
      y_pos = (max(.data[[yvar]], na.rm = TRUE) -
        min(.data[[yvar]], na.rm = TRUE)) * 0.95 +
        min(.data[[yvar]], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      label = paste0(
        "  y = ",
        round(intercept, 2),
        ifelse(slope >= 0, " + ", " - "),
        round(abs(slope), 2), "x",
        "\n  R² = ", round(r2, 2),
        "\n  p ", ifelse(p < 0.001, "< 0.001", paste0("= ", round(p, 3)))
      ),
      label_simple = paste0(
        "R^2~'= ", round(r2, 2),
        ";'~p[adj]~'",
        ifelse(p_adj < 0.01, "< 0.01", paste0("= ", round(p_adj, 2))),
        "'"
      )
    )
}

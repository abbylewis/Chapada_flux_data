#' Make lm stat labels for plots
#'
#' @param data Dataset to use
#' @param xvar X-axis variable
#' @param yvar Y-axis variable
#' @param group_var Grouping variable 1 (if applicable)
#' @param group_var2 Grouping variable 2 (if applicable)
#'
#' @returns Data frame with stats and labels for plot

make_gam_label <- function(data, xvar, yvar, group_var = NULL, group_var2 = NULL) {
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
      model = list(
        mgcv::gam(
          reformulate(sprintf("s(%s, k = 3)", xvar), yvar),
          data = cur_data(),
          method = "REML"
        )),
      r2 = summary(model[[1]])$r.sq,
      p = summary(model[[1]])$s.pv,
      x_pos = min(.data[[xvar]], na.rm = TRUE),
      y_pos = (max(.data[[yvar]], na.rm = TRUE) -
        min(.data[[yvar]], na.rm = TRUE)) * 0.95 +
        min(.data[[yvar]], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      label = paste0(
        "  R² = ", round(r2, 2),
        "\n  p ", ifelse(p < 0.001, "< 0.001", paste0("= ", round(p, 3)))
      ),
      label_simple = paste0(
        "R^2~'= ", round(r2, 2),
        ";'~p~'",
        ifelse(p < 0.01, "< 0.01", paste0("= ", round(p, 2))),
        "'"
      )
    )
}

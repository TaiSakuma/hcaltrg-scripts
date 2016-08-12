#!/usr/bin/env Rscript
# Tai Sakuma <sakuma@cern.ch>

##__________________________________________________________________||
argv <- commandArgs(trailingOnly = FALSE)
olddir <- setwd('mianRs')
source('drawFuncs.R')
source('drawThemes.R')
source('all_outputs_are_newer_than_any_input.R')
setwd(olddir)

##__________________________________________________________________||
eval(readArgs)

##__________________________________________________________________||
library(latticelog)
library(tidyr, warn.conflicts = FALSE, quietly = TRUE)
library(dplyr, warn.conflicts = FALSE, quietly = TRUE)

##__________________________________________________________________||
theme.this <- function()
  {
    cols <- c("darkgreen", "orange", "#ff00ff", "#ff0000", "#0080ff", "#00ff00", "brown")
    col.regions <- colorRampPalette(brewer.pal(9, "Blues"))(100)
    theme <- list(
      add.text = list(cex = 0.8, lineheight = 2.0), # text in strip
      axis.text = list(cex = 0.8),
      axis.line = list(lwd = 0.2, col = 'gray30'),
      reference.line = list(col = '#eeeeee', lwd = 0.2),
      regions = list(col = col.regions),
      background = list(col = "transparent")
    )
    modifyList(theme.economist(), theme)
  }

##__________________________________________________________________||
arg.tbl.dir <- if(exists('arg.tbl.dir')) arg.tbl.dir else 'tbl_01'

##__________________________________________________________________||
main <- function()
{
  sub <- function(varname, xlim = NULL, adjust = NULL)
  {
    tblFileName <- paste('tbl_n_component_', varname, '.txt', sep = '')

    tblPath <- file.path(arg.tbl.dir, tblFileName)
    if(!(file.exists(tblPath))) return()

    fig.id <- mk.fig.id()

    figFileNameNoSuf <- paste(fig.id, varname, sep = '_')
    suffixes <- c('.pdf', '.png')
    figFileName <- outer(figFileNameNoSuf, suffixes, paste, sep = '')
    figPaths <- file.path(arg.outdir, figFileName)
    
    if( (!arg.force) && all_outputs_are_newer_than_any_input(figPaths, tblPath)) return()
    dir.create(arg.outdir, recursive = TRUE, showWarnings = FALSE)

    tbl <- read.table(tblPath, header = TRUE)
    colnames(tbl)[colnames(tbl) == varname] <- 'val'

    if(!is.null(adjust)) tbl$val <- adjust(tbl$val)

    tbl$component <- NULL

    ## to draw right side vertical line for the first entry
    tbl <- rbind(c(min(tbl$val), 0, 0), tbl)

    theme <- theme.this()

    p <- draw_figure(tbl, varname, xlim)
    ## p <- useOuterStrips(p)
    
    print.figure(p, fig.id = figFileNameNoSuf, theme = theme, width = 4.5, height = 3.3)
  }

  sub('QIE10_charge')
  sub('QIE10_energy')
  invisible()
}

##__________________________________________________________________||
draw_figure <- function(tbl, varname, xlim = NULL)
{
  y.at = -5:10
  y.labels = log10.y.labels(y.at)

  ##________________________________________________________________||
  prepanel <- function(x, y, type, subscripts, groups = NULL, ...)
    {
      ret = prepanel.default.xyplot(x, y, type, subscripts, groups, ...)
      if (!is.null(xlim)) ret[['xlim']] <- xlim
      ret
    }

  ##________________________________________________________________||
  panel <- function(x, y, subscripts, groups = NULL, ...)
    {
      lim <- current.panel.limits()

      y[is.infinite(y)] <- lim$ylim[1] - 1


      if(length(x) == 0) return()
      panel.grid(h = -1, v = -1)
      panel.xyplot(x, y, subscripts = subscripts, groups = groups, type = 's', ...)
    }

  golden_ratio <- 1.61803398875
  ##________________________________________________________________||
  xyplot(log10(n) ~ val,
         data = tbl,
         xlab = varname,
         ylab = 'n',
         aspect = 1/golden_ratio,
         prepanel = prepanel,
         panel = panel,
         scales = list(
           y = list(alternating = '1', at = y.at, labels = y.labels)
           ),
         )
}

##__________________________________________________________________||
main()

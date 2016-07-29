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

  tblFileName <- 'tbl_custom_hfprefechit_energy.txt'

  tblPath <- file.path(arg.tbl.dir, tblFileName)
  if(!(file.exists(tblPath))) return()

  fig.id <- mk.fig.id()

  figFileNameNoSuf <- paste(fig.id, 'levelplot_energy', sep = '_')
  suffixes <- c('.pdf', '.png')
  figFileName <- outer(figFileNameNoSuf, suffixes, paste, sep = '')
  figPaths <- file.path(arg.outdir, figFileName)
   
  if( (!arg.force) && all_outputs_are_newer_than_any_input(figPaths, tblPath)) return()
  dir.create(arg.outdir, recursive = TRUE, showWarnings = FALSE)

  tbl <- read.table(tblPath, header = TRUE)

  tbl28 <- tbl[abs(tbl$ieta) == 29, ]
  tbl28$ieta[tbl28$ieta == 29] <- 28
  tbl28$ieta[tbl28$ieta == -29] <- -28
  tbl28$energy <- NA

  tbl <- rbind(tbl, tbl28)

  tbl$idxQIE10 <- factor(tbl$idxQIE10, levels = c(0, 1), labels = c('QIE10 index = 0', 'QIE10 index = 1'))

  theme <- theme.this()

  zmax <- ceiling(max(tbl$energy, na.rm = TRUE))
  zmin <- floor(min(tbl$energy, na.rm = TRUE))

  z.at <- zmin:zmax
  print(z.at)
  print(length(0:zmax))
  print(0:zmax)
  print(length(zmin:0))
  print(zmin:0)
  print(length(z.at))
  col1 <- rev(colorRampPalette(brewer.pal(9, "Blues"))(-zmin))
  col2 <- colorRampPalette(brewer.pal(9, "Reds"))(zmax)
  print(col1)
  print(col2)
  col.regions <- c(col1, col2)
  theme = modifyList(
    theme,
    list(regions = list(col = col.regions))
  )

  p <- draw_figure(tbl, z.at = z.at)
  ## p <- useOuterStrips(p)
   
  print.figure(p, fig.id = figFileNameNoSuf, theme = theme, width = 6, height = 5)

  invisible()
}

##__________________________________________________________________||
draw_figure <- function(tbl, z.at)
{
  levelplot(
    energy ~ ieta*iphi | idxQIE10,
    data = tbl,
    aspect = 3/8,
    between = list(y = 0.5),
    ## pretty = TRUE,
    at = z.at,
    scales = list(
      x = list(alternating = '1', tick.number = 15),
      y = list(alternating = '1')
    )
  )

}

##__________________________________________________________________||
main()

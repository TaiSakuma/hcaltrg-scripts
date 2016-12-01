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

  tblFileName <- 'tbl_Scan.run.lumi.evt.ieta-wp.iphi-b1.depth-b1.idxQIE10-b1.eta-b1.phi-b1.energy-b1.energy_th-b1.txt'

  tblPath <- file.path(arg.tbl.dir, tblFileName)
  if(!(file.exists(tblPath))) return()

  fig.id <- mk.fig.id()

  components <- strsplit('e030 e050 e070 e100 e150 e300 pi030 pi050 pi070 pi100 pi150 pi300', ' ')[[1]]

  figFileNameNoSuf <- paste(fig.id, 'levelplot_energy', components, sep = '_')
  suffixes <- c('.pdf', '.png')
  figFileName <- outer(figFileNameNoSuf, suffixes, paste, sep = '')
  figPaths <- file.path(arg.outdir, figFileName)

  if( (!arg.force) && all_outputs_are_newer_than_any_input(figPaths, tblPath)) return()
  dir.create(arg.outdir, recursive = TRUE, showWarnings = FALSE)

  tbl <- read.table(tblPath, header = TRUE)
  tbl$energy <- tbl$energy_th

  evt <- sort(unique(tbl$evt))[2:7]
  tbl <- tbl[tbl$evt %in% evt, ]
  tbl$evt <- factor(tbl$evt)

  tbl28 <- tbl[abs(tbl$ieta) == 29, ]
  tbl28$ieta[tbl28$ieta == 29] <- 28
  tbl28$ieta[tbl28$ieta == -29] <- -28
  tbl28$energy <- NA

  tbl <- rbind(tbl, tbl28)

  tbl$depth_idx <- paste('depth = ', as.character(tbl$depth), ', idx = ', as.character(tbl$idxQIE10), sep ='')
  tbl$depth_idx <- factor(tbl$depth_idx)

  theme <- theme.this()

  zmax <- ceiling(max(tbl$energy, na.rm = TRUE))
  zmin <- floor(min(tbl$energy, na.rm = TRUE))

  z.at <- seq(from = zmin, to = zmax, length.out = 50)
  len_negative <- length(z.at[z.at < 0])
  len_positive <- length(z.at[z.at >= 0])
  col1 <- rev(colorRampPalette(brewer.pal(9, "Blues"))(len_negative))
  col2 <- colorRampPalette(brewer.pal(9, "Reds"))(len_positive)
  col.regions <- c(col1, col2)
  theme = modifyList(
    theme,
    list(regions = list(col = col.regions))
  )


  for(component in components)
    {
      tbl_<- tbl[tbl$component == component, ]

      p <- draw_figure(tbl_, z.at = z.at)
      p <- useOuterStrips(p)

      figFileNameNoSuf <- paste(fig.id, 'levelplot_energy', component, sep = '_')
      print.figure(p, fig.id = figFileNameNoSuf, theme = theme, width = 8, height = 5)
    }

  invisible()
}

##__________________________________________________________________||
draw_figure <- function(tbl, z.at)
{
  levelplot(
    energy ~ ieta*iphi | depth_idx*evt,
    data = tbl,
    aspect = 3/8,
    between = list(x = 0.2, y = 0.5),
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

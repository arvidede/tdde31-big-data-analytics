
smoothingFactor <- c(2.5,50,2000)

xTime = seq(-10,10,by=0.01)
xDays = seq(-183,183,by=1)
xDist = seq(-700,700,by=1)

iterList <- list(xTime, xDays, xDist)
titles <- c('Time [h]', 'Days [n]', 'Distance [km]')

kernel <- function(u) {
  return (exp(-(abs(u)**2)))
}

par(mfrow=c(1,3))

iter <- 1
for(factor in smoothingFactor) {
  y <- c()
  x <- iterList[iter][[1]]
  for (i in x) {
    val <- kernel(i / factor)
    y <- c(y, val)
  }
  plot(x, y, type='l', xlab=factor, ylim = c(0,1), main = titles[iter])
  iter <- iter + 1
}
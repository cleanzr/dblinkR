library(sparklyr)
library(dblinkR)

sc <- spark_connect(master = "local", version = "2.3.1")

projectPath <- "/home/nmarchant/dblink/"

records <- read.csv("/home/nmarchant/Dropbox/Education/PhD/bayesian-er/dblink/examples/RLdata500.csv")
records['file_id'] <- 1
records <- lapply(records, as.character) %>% as.data.frame(stringsAsFactors = FALSE)

distortionPrior <- BetaRV(1, 50)

levSim <- LevenshteinSimFn(threshold = 7.0, maxSimilarity =  10.0)

attributeSpecs <- list(
  fname_c1 = Attribute(levSim, distortionPrior),
  lname_c1 = Attribute(levSim, distortionPrior),
  by = CategoricalAttribute(distortionPrior),
  bm = CategoricalAttribute(distortionPrior),
  bd = CategoricalAttribute(distortionPrior)
)

partitioner <- KDTreePartitioner(0, c('fname_c1', 'bd'))

state <- initializeState(sc, records, attributeSpecs, recIdColname = 'rec_id',
                         partitioner = partitioner, populationSize = 500L,
                         fileIdColname = 'file_id', randomSeed = 1,
                         maxClusterSize = 10L)

result <- runInference(state, projectPath, sampleSize = 100, burninInterval = 1000)
result <- loadDBlinkResult(sc, projectPath)

diagnostics <- loadDiagnostics(sc, projectPath)

linkageChain <- dblinkR::loadLinkageChain(sc, projectPath)
mpc <- dblinkR::mostProbableClusters(linkageChain)
smpc <- dblinkR::sharedMostProbableClusters(linkageChain, mpc)

clustSizeDist <- clusterSizeDistribution(linkageChain)
partSize <- partitionSizes(linkageChain)

predClusters <- smpc %>% dblinkR::collect()
trueClusters <- exchangeableER::membershipToClusters(records$ent_id, ids = records$rec_id)
predMatches <- exchangeableER::clustersToPairs(predClusters)
trueMatches <- exchangeableER::clustersToPairs(trueClusters)
numRecords <- nrow(records)
conMat <- exchangeableER::confusionMatrix(predMatches, trueMatches, numRecords*(numRecords - 1)/2)
exchangeableER::pairwiseMetrics(conMat)

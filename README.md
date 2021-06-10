<!-- README.md is generated from README.Rmd. Please edit that file -->

# dblinkR: An R interface for dblink

## Overview

`dblinkR` is an R interface for
[`dblink`](https://github.com/cleanzr/dblink)—an Apache Spark package
for performing unsupervised entity resolution. It implements a
generative Bayesian model for entity resolution called `blink` (Steorts
2015), with extensions proposed in (Marchant et al. 2021). Unlike many
entity resolution methods, `dblink` approximates the full posterior
distribution over the *linkage structure*. This facilitates propagation
of uncertainty to post-entity resolution analysis, and provides a
framework for answering probabilistic queries about entity membership.

## Installation

`dblinkR` is not currently available on CRAN. The latest development
version can be installed from source using `devtools` as follows:

``` r
library(devtools)
install_github("ngmarchant/dblinkR")
```

### Dependencies

`dblinkR` depends heavily on the `sparklyr` R interface for Apache
Spark. Please refer to the `sparklyr`
[website](https://spark.rstudio.com/) for information about connecting
to a Spark deployment.

`dblinkR` currently supports Spark releases in the 2.3.x series and
2.4.x series. Spark releases prior to 2.3.x are not supported.

## Example

The [RLdata500 vignette](vignettes/RLdata500.Rmd) demonstrates how to
use `dblinkR` to perform entity resolution for a small synthetic data
set. This example is small enough to run on a laptop (Spark cluster not
required).

## Licence

GPL-3

## References

<div id="refs" class="references hanging-indent">

<div id="ref-marchant_d-blink_2021">

Marchant, Neil G., Andee Kaplan, Daniel N. Elazar, Benjamin I. P.
Rubinstein, and Rebecca C. Steorts. 2021. “d-blink: Distributed
End-to-End Bayesian Entity Resolution.” *Journal of Computational and
Graphical Statistics* 30 (2): 406–21.
<https://doi.org/10.1080/10618600.2020.1825451>.

</div>

<div id="ref-steorts_entity_2015">

Steorts, Rebecca C. 2015. “Entity Resolution with Empirically Motivated
Priors.” *Bayesian Analysis* 10 (4): 849–75.
<https://doi.org/10.1214/15-BA965SI>.

</div>

</div>

# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       48 |        9 |       12 |        4 |     78% |21-38, 53, 76->75, 85->84, 93->92, 94 |
| em\_workflows/constants.py                |       12 |        2 |        4 |        2 |     75% |     9, 11 |
| em\_workflows/czi/config.py               |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/constants.py            |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/flow.py                 |       88 |       50 |       24 |        3 |     37% |22-24, 28-41, 45-84, 95-117, 121->120, 122-125, 129->128, 130-135, 138->exit |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      113 |        6 |       36 |       10 |     89% |17->16, 30->29, 49->48, 68-69, 102->101, 118->117, 136-137, 175-176, 215->221, 215->exit |
| em\_workflows/file\_path.py               |      109 |       12 |       22 |        9 |     82% |66->65, 75->74, 93-94, 113-114, 141-143, 228-229, 239->238, 250->254, 255-257 |
| em\_workflows/lrg\_2d\_rgb/config.py      |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/constants.py   |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/flow.py        |       70 |       23 |        8 |        5 |     64% |22->21, 45->44, 76-88, 92->91, 93-127, 130->136, 130->exit |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/utils.py              |      268 |      107 |      108 |       24 |     57% |61->60, 71-101, 105->104, 119->118, 125-126, 130->129, 160->162, 163, 171->170, 180, 256, 285->287, 298-309, 328->327, 350-379, 510->509, 549->548, 567->578, 569, 579, 586->585, 592-597, 673-687, 699-733, 754-769, 790->789, 801->803, 810->809, 940->939, 957-973 |
|                                 **TOTAL** |  **744** |  **209** |  **214** |   **57** | **67%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/niaid/image_portal_workflows/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/niaid/image_portal_workflows/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fniaid%2Fimage_portal_workflows%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.
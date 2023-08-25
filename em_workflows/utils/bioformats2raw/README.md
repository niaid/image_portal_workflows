This Dockerfile was built for mac users in mind who has issues running `bioformats2raw` command on their machine.

The Dockerfile is using the bioformats2raw version: 0.7.0_ currently.

In order to build the docker image, use `--platform linux/amd64` option.

```bash
$ docker build . -t hedwig_bioformats --platform linux/amd64
```

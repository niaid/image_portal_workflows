This Dockerfile was built for mac users in mind who has issues running `bioformats2raw` command on their machine.

In order to build this image, you need to download one of the releases.
[0.7.0 Release](https://github.com/glencoesoftware/bioformats2raw/releases/tag/v0.7.0)
Place the `zip` file in the same directory as the Dockerfile to build it.

The Dockerfile is using the version: _0.7.0_ currently and can be updated as per necessary.

In order to build the docker image, use `--platform linux/amd64` option.

```bash
$ docker build . -t hedwig_bioformats --platform linux/amd64
```

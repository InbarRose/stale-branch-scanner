FROM python:3.7 as base

WORKDIR /app
ENV REPOURL=https://github.com/InbarRose/stale-branch-scanner.git
ENV REPONAME=stale-branch-scanner
ENV MAINBRANCH=main

# add src code
COPY scan_unmerged_branches.py /app/

# TESTS
# 1. checkout this repo and scan it
RUN git clone ${REPOURL}
RUN python /app/scan_unmerged_branches.py ${MAINBRANCH} /app/${REPONAME}

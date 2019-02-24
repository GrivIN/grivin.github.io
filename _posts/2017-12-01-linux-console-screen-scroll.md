---
layout: post
title:  "Scrol output in screen"
date:   2017-12-01 16:16:01 +0100
categories: screen
---

To move up in screen session you have to use `copy mode` similar to vim block mode.

* Hit your screen prefix combination (`C-a` / `control+A` by default), then hit `Escape`.
* Move up/down with the arrow keys (`↑` and `↓`).
* When you're done, hit `q` or `Escape` to get back to the end of the scroll buffer.

(If instead of `q` or `Escape` you hit `Enter` or `Return` and then move the cursor, you will be selecting text to copy, and hitting `Enter` or `Return` a second time will copy it. Then you can paste with `C-a` followed by `]`.)

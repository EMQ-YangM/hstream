#!/bin/bash

FORMATER_BIN=${FORMATER_BIN:-stylish-haskell}

find . -type f -not -path "*/dist-newstyle/*" -not -path "*/.stack-work/*" \
     -not -path */hstream-sql/src/Language/SQL/Abs.hs                      \
     -not -path */hstream-sql/src/Language/SQL/Lex.hs                      \
     -not -path */hstream-sql/src/Language/SQL/Par.hs                      \
     -not -path */hstream-sql/src/Language/SQL/Print.hs                    \
     -not -path */hstream-sql/src/Language/SQL/ErrM.hs                     \
  | grep "\.l\?hs$" | xargs $FORMATER_BIN -c .stylish-haskell.yaml -i

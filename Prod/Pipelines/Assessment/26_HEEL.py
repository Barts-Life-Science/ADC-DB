# Databricks notebook source
import base64, gzip, hashlib, json, re

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
dbutils.widgets.text("source_update_id", "")
dbutils.widgets.text("silver_update_id", "")
dbutils.widgets.text("scratch_prefix", "")
RUN = dbutils.widgets.get("run_id").strip()
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts").strip()
SOURCE_UPDATE_ID = dbutils.widgets.get("source_update_id").strip()
SILVER_UPDATE_ID = dbutils.widgets.get("silver_update_id").strip()
SCRATCH_PREFIX = dbutils.widgets.get("scratch_prefix").strip()
assert RUN.startswith("dq4_omop_") and RUN.replace("_", "").isalnum(), RUN
assert RUN_OPEN_TS and SOURCE_UPDATE_ID and SILVER_UPDATE_ID and SCRATCH_PREFIX
LANE = "heel"
PREFIX = "heel_" + RUN + "_"
SESSION = "DQ4"
PAYLOAD = """H4sIAAAAAAACA+1djXPbtpL/VzCZubGdyjYBkiLlSzLj2ErrO8d2bad9vaajoSTY4itFKiRlR+/N+98PH6TED0imRICyW3emoQWS4GKx+8PuYgH8+83ECR3Pw96bI/D7v9/cuR4mf70ZfjsMpvHh8JvRC8bBpIc01NZsZPZCdDjC2DuM4nEcHUbTySQI494Qh+4DHvZguaSnkf8Oom/emxZ4Qy+k9pPr7vFtF9wefzzvAps8+3AQud4DDnvfBr14PDmgn+iJPt6DX/0vN2cXP4LT7vnt8Vf/+Oarf9M9757cfvUHThTv+lPPA04EXD/eo1fHd7xZ5EY9d9j66oMoDp14Ou5B+iP3ws3tNamXvZM+hNhDwdSPd98mN5zYjWJ30HtwvCme17HzxR87kwkenjqx83F2Gowd1z+6CabhAP9Cnzzx453CR8bYiaYhJnR99T9dX37+6os4cXzy09n5efemF+Jo6sUReBzhEGdbBd4D2NE08NW/D4PpBPRni0a++U8L1O9UXVAku1v1mv3KnslyRdyj1bv95PLLxe2Sbgfzfj8ejFyiPdFRf3acfPzoOniU192ZXs20Tk6/GoIi2f1q1OxX5yDXdYufRllRaXXwQHvrHExwGAV+b+DHh/2DSTCZeqQDSUHk/gvT904vv5C2LFFp3rO/uJEbH93OJvjoitX2qxuPjuNz0o3xpY9Jh5/iAekAeneA/di5x093+W6EPTyIQQ6Isr8M/ms63mWww2lilS2aRJ+4C4MxqChAQshAmkFvlSGjtaCFfPer7/o+DsE/A9cHc/qrEZxnu0qiCaX9r36QE4/3/cXfX/2ktoxggA8ENyUBpCkokq1IpgyAXAF6cgbEBTKe4gd3gI9OAn+AJ/FKQAS1B0AENUld2RYUye7K9gvrys+8p8YE5RrqT2jL6k9LUCS7P60X1p+X/QiHDwyaG+pPad1pC4pkd6f9wrrzKgwGeEi6qqHObMvqzI6gSHZndl5YZ14EcWODJpLUkahcIurG0+vLq6QTzz6B7j/Obm5v1uzOGI8nPU0l0VCe7HFiV8hfVtREBjhqCQ3rkvAI/Axqk89djcpyfucFQbjL/pyTk75zCLU9OTpQjJ0c3+Pj+JMbEr+qH33MuFXEUn/CkV7DpRCaGRr86u+Bn8VO9rI+WRCgUhCRIu1RqvK6ZO2BLwK9n5Bkftk4JPQ0wHDRzgwN78HTSkXhZwjEug7evbdViokhT0xQTXxdC0vrVrgcrp7uZDD1XRrh8LxnQA+DzWOVImIWRCQOp0TjYwxip+/hZ2c2tAvkDsNg8kxJtaRzVumQYkvlrCRSNUGRZM8FaS8u4OfEoyP274kzjZryYkxZTgwUFMnuU/hy+5TNhDTVpaacLhVACpLdpajxCWkGZHQGY8XsFe+4/uw37IRHN84dviLPYT+uZIWms1QL0kRu6JyM2m6YuWwuihSyOSj+0vyD8mZukC4oki0hUqa2h3jgDHFttV9Hdk7ZJzeWHg/fZVyLls4e581oWJ5KlKiXK0NQJFuuJE+tVxs2FnPs/SBMZ9iHLmlgIlUrp9Zzw8pHUgGRMSJAeHhNZyM2nD/Px5XoiylpUmbKqeAC8Tx4eR6fD6tF0Z7zRwY9VLrLULkH+qAwBQ7Jw4tJcJiKe6a7PgBJnoYpKJIt7ubWIqn7+5C2Rfsh95m5G14xwjqnRUro9OT45pa0h3puuzvOYNTbabEicQpUC+wc/egFfcejEL4nJ6IqlE+iHrvQ1syWRf5vk//p34Zm7oFnEGltC4pki+nzmdyvJPHri1IqSUma256sPDexLBkaJGIEiThBIkqwRTMD9oCynDhkCYpki8jzyRdg/UzAjQzoGUk5nEvJQRzEjtebkA46vnliaGd1nVxenBzfVhOjTOLcHq1ehhANwiCK+Ci9y1kKisbBoknVoS4UjcQQnJ99PrsFMM+RZZKMiBC1yD+Q/oPoP/qeHJm1BUWyZfaZJUUsuvNJRMumZh9xuVyRLKwM3vb3by9PL48WxpkbgQcnHIycENCZmHhECgbBeOKEbkTsuLEzA3eO6wHydxSMMRj2AfbvXR9HtDLXH2JMBs7xjHQuuA9igMMwCFuAVOjfY1JfQMxW8jXXv89Uux7MUrqy1uSOtiNHYjuCItkS23lxA3E28fjMJ0/4A8ULB2RlgOiaoEhyh+ovLSB+EvhDt8EkO0NWZ0JBkezOfGmRcK6VDSVmyepIJCiS3ZHopWVLshSIo2488t2BG8+amtOQ06O6oEh2j+ovs0evncaWGxhSOrMcjZAdjGi/rPSpqzB4cAkvjm4meOA6XjxTa/3oknC2HDOQHTJ4aSsMTsPpfff7JIiay0q3JHVm2ZmW7UvbL7EzQ6ehfuxs2I+aBnvpWvZeOPVwD4rKRH25v//T2cUtOD2jTfr45bbbu7zo/W/3t2yIgMava/Q5/3jFGQzC+Tmrfup2z3uPTugTP57eYBXxZ0I8CMJhj3VidroiiXvR5pxdkD+CEB4Uqs/F6rrX15fXR2Cnxfu78Hh+FmGfPBU4mQd8Z0xkZYeLEtj132erKYRqFtXsgWgUTL0h8IMY9DGdiMrHAMmfS1nA0lQzfBB8Kc8aQHlTLdBH6iLPn11cdK/B/1yeXax+K+UCZQl57fKiyGsizzlusRmWX3/qXndLT5Jv0c6zaIts+k+HtRUa/GLyC7uDNItfkl/sSQQ1etE1ptkGLzR4oQEhv/B7UKcXkz+yv2+Sh0Ds/IkJK6cxj4qyaNR03A+x5zmM2SwY5I4nHlsHiIcgwlS1YuzNSC1tXlmbf6/NP9TmH7L4PYvfszgtFn/E4o/Y/BGbP2Lze3Zyj7Ogw1vb4U92+JNQ46XkytmlJeVGwjBT05Ir+ypsJ7/byW9L40km+/sEJAD+PvGoU8ClmXTrHRg6sUMRahx4Lo28geOL05LEEfFltRyrRq7i6pTLq9uzz2f/tykagf+7vD4lkv7xt9oTKZqGCgQjUdk24Rc1Cr/k3pqwO1yBu2AfLENeHlqk0dmkJr5zBatv7PqZmGMWf8kdwIU3j8Q+vid2wAOuDseoAMdLP78pKPeGxC5h/KwJzcPq2DwUgjPUdI4yJgfeNofaNodTdmnzi5VcOAxZMPnF0Z3f66QXK0GwdnK1EsRCyVVPrkZyTZGtnVzT5+3kypHQTJDQhCnyoeSqJ1cjuZrJtZ1creRqU0ybI162O8E7Bng/Xl9+uaLgUZL1sqRuvmBgDWSRCY8ELYBUfNQLFOuism3io74t8/QJoPz1+PqCkFUVKvc3Akrn+zKgdL4LgZI4ty4FSvKxmLgxj26EgUtsBgf8Kxj3XQwe3XjEzYcPcBwA5y7GIRjSzDdQHVz1FeCaI/nlg2sCVCY3/szESOXGn0m3PymAUdp8Yn21m0SjynosE410ycaaUSDYEJVtE4yMho21BlzkPOzM8S+TwrkABe5Ss3gLjQMlbkiIGfpQ0y0YOP2p54Sz6lhiCLFERMdz96DPu59ueeV6zwuCP6eTAypFBwnHwCBTVzZ/geeFwPQ5Wv2Cd0955tx3ZmjErb/E3+auNf9lpheT24Lc4+UXm186Wuq5pp4ot9eI65yDuCzlZzfg4vIWXHw5P0/u5xtB7/N7GRAsSbRUFKwMIDJR0JCMgmaBYFNUtk0UNP92KIhUo6BZDQXRXxMFUV0UNFJo444tv9iJD0muSzAMPUcMq6z+MjHMlIxh7QLBbVHZNjGs/awwTOBMboJiDImYhzdN0kvnQFUditoFKMrN2fXd+3T+7+XOZmzRZmLZqk2iSWVFlIkmbcloYhUItkRl20QT69UvXGURkdaEdGHiJpaR9ZfxDzO1N+Ugos08t3cfAN25nd3xgkcc0q8P2fbxTGHoy7T6nXvsD3G4s9ckolUGA5mIZklGNLtAsC0q2yai2a+IpgrR7FdE2xzRDMWIFjoD3CyeVYYCmXhmS8azToHgjqhsm3jWecWz9fAM7J58vgHzBQl71fGt84pvm+ObqRjfcNqhzYJcZXyQCXIduSAHtWLymyYs3Goyr/aKc6rsNqi9AlsNV1RT7Yw+0MWQjcLaOoggNUlWk4xspbReKCzcKrLBV2Rb14Kbr1hbw3qD8BXlNkc5XYOKUW6SLEcEUdq5zULeGlAhFfIkLwyAxVRdiISFW4U89Ap5yow59ApzNVMu5rOfG2eIVQa9QbppyU5r8ffhMJzeE3nKlNAln/mSoF8omKTnUjULm2vAjVTYRJJhs5hTDHVh4VZhU3+FTWWwqb/CZi3YtOa5I+pRk+OjADCH7BTUxU9yPRwvjtOclyXgyf7eEm6ugTdScVPy0gZYzEKGhrBwq7j5urpBHW4ar7i5OW62NU29V52gW0toKy5AM1+WR0ZSQiBzW1C5BsRIhUrJ6x9gMdkZWsLCrULla8Lf2sHIED9gf4rJvSFeJx75mgBYAzlpMrLqjJlMvzYLeGsAhVTAk5wMCIv52NAWFm4V8Oy/8C5RmSX0I+cBgxl2QrrLTt8N4xGFs3iEwd00JiN6K7uZFN1BubDpTnGh/ho4V0wLXFb7C113oacwxPovj0WEL0SK98AHxvrdky/X192L294pEdm9lZsbNQY1a6ioVKiRnKcHi4nSsCMs3CrU/AVT9VYjzDu6RlFTCC2dvwm0DJZDC+fx8wCTNZRQKpjIzocrZikjTVi41d3VtL8+mDj3bOsrdfiBtL82fsBF0sbJKgR5JvCxhtpJ3XpMdtJZMf8XQWHhVuED/j3g4wPdoU8hgMBXANnjTH4eELKG6kmFEMlJXKiYaouQsHCrECI5iavadgzLIyrOultROE+hjCNCGbr7IKb7DkJN+6/k+C8aShkHfjzyZsnO3MQFotHDdbanQNnErUoYMdd2x1kHJFJoccIEIxyniBEF1mxUOUoqD8uVo0LlFDicJdlYiG9xzS8Wv9j80kFJphZKcw44ku2mXe8I4eoHMA89U1JKD1C6r1dXYHeeqiGFTXiggbfA6UfsySwy7rMm5waKw2IRhdYMfwp3tBpIuQ7CSEVKyXlbqJihi3Rh4VaRstm8rYaAcMm0Wk5llk6tcQNNPoY+kcVVIG4Tw+wvB7rZ+bgcmj0BzHyD62Sfa4tfbH7pJJtfaxoqATOxBC9Od7lAFiEWsWNvuydnn4/Pd6HdMvb2MgfhtrQUu8t1oOp1cHivSwYbAerS0dAgsTC/BdDgSDS/1wFjqYOK5KQ2VNru3BAWbnVQMV73D19j//APNEy+xkBi/E12AreguXSfb8Kx5jb6XkfnpGKH5CwvVMzhRaawcKvYYb5ix3rYsQ50mH8b6Ggvh44mkWMNjZOKHJL3lkXFlFbUFhZuFTnafzfk4Ic2bowcbW0d6ChtUbvs4y8eOqwVp4s0iR1r6JxU7Fixk+wf9BM4dB36ld83aJPZ468nn7IERSIMOb2+vEqQ4ewT6P6D6N7NBm0jlW94pGhVwqG8w2FTgleg2tssLkXYo+nfb/mxjet9yxmMXCJTUY8oLhXxqe8GxP/2vHm9xVmQJSfSMuDbOZkvHc7CS+5gWlD1ZFpWIyQSob2N4gOiiYeF82kzoYslZ0pzor4k23OfOrFz1J+dsrTqoyscDrAfO/e4SGv2eGkZPB2EQRSBfwauD3aTviq2hHyV/CHjY/w83cxpvJ77JwZ5HpB3eoYGj370gr7jXQeP9DzfhIeiKsjAI3yFYtzPatUKKcMDpJZwXToeoCbwYIE9daHgar4uaykUNI0EoHEoSNkJjlXjwPxLSxS4BADtdQBgR/hGA/pvKNN/XS3hpnT91xvUf1Rf/9mR8d8nQfQKAYyjjUEAqgwB1toQYG0BAtrKIMBQS7glHQKMBiFArw8Bl33ClAfnmfkDW4MAvTEI0KsigL02AthbQABbGQKYagnvSEcAs0EEMOojwOfMPjevCNAzGkMAoyoCwPUhAG4BA6CCwGD6kSH5hFriJQYHs0SvwgIgGQzMBvq4GOyJw6k/cGIMYqfv4ecY9oXFMM8wDCbPmFxDAYfVBtKgKZnDisltK+Cw2lAFtCRzWDG5tgIOq/UEYUcyh9WSizQFHFZraSMomcOKyUXKrJlRqNqaQbp0a4YR3eh0J/licbozSe9Yx79JEz0uLm/PPp2RP88uL45SE5vv0xUQusBk7liAeBTiaBR4Q1Lx0c7C9xlmct4XLtBewQ9ZnqVhST5IuJoFCoZzN2F4sMLTELlYNM/bJ1w6KDo/H4B2AMH+/pxVEXgkvUZzV4Z44A7xMN0mKPDviRd1D05/3r+Jp8MZUm+kIkMW0uRkUS3NpnSa6WzzhjS38wTagiJFmSAxHk+uec6RWtol+nsZmtd1974u4gvZ9RdLYXPRucnSHa7Y+ZQqaCO495YFYQ4JbnIgWZl+QWNARxfBxXT8C72xVpSFNmHRILqQvbiIfUFD7XaxBZsghhREYqRWQpDScIatlnhdSTjDbiSckY+fCOKbVQ2ApyKW1ec2BZpTN96YQ43Nh8SK8mAotWYVC7OpxJq1m7Bm89ZzDVnmQJ2zYrM2a3AH/MDf96dj8sGB44HM9vWJQRkB/H2AMbneYx+H5JlJMJl6bO4vY/PuVLZnbZE9KwiTL7Fr6+kMoFXs7/Nk4ViwA4lm6VqLXAxd2yMPTsf9EHueA8iHItLieaJx3iReNRISc1f0HY1vxk2NYboaOaZHwA+zTkQEYNpVLVLuRknON/mD7QoB4oB0SQwmrhfE3CVhvMFO6M2AMwwmMaE5yXuOy7b4e2BrDUCI1LBZY3aetMhZfYqtPHkdQdFSq/qXs+6vGyP1g4sfGf0DP1ZL+xKrmi6c7l6dH590eUM2Jx9QdNpdYWxO4t5glaVZWvw+947zxqahGUSlh99Bf8ZcWg8nznB23fsO7JiaZVJoGBNw893pmCFB9jmdPmeTx+hTkyCKXCp7njt2Y6r4Y4eYvcRL9mZJ/QmK8peNOX4SLTdJBXfuXTxKCAJBCMZBSNcxqO1V9Bz9vIq064r8vP39iED2AuWpCLXAYBqGBOYJbJPeJfj+LxwGAjNjN7UzEmktDI5zS7qGnnCZTur/ADTFIqLWvuyoJV6Nfdlp2L60JdiX6XK43wlYE+I/np2f3f72B3nnzvHjfWpoDl3n3g/oqq5dDn6kspjtmWdqP1S3HTubxUK352dVFKVnZiRVpPoZGUl2jjwdCooUhR4XYQcdqiVeSa6JTtcVNBudsZ9JdIYnnR1obwfxQRzEhLoJ6Y5sQHV10hnIrohhhwkfXZEnyECe/r6mXvJmGWfF0KY4uyxLLKl63oo1QqahyI6Fe2AQLzFxda2W21hR2JXOo6rWVCXzqDps2DLoyI48FcwDGrLoOxEGIya6AbjDjyA9lzsi3sqd6+Mh2L13HzCfFWTyDSZcyYA/HfdxuFfUr8XsG21kxnrQYWPWQw5fl8+krsaNFdOoLH7U/ERqJy/rSFCkdpSlaoDUEg+V6C5qVnd1KD1qfJWLGic6SMUuHgE/AA9u5MaLWPFcMKtrJ3pu2kkzohEBc0FqA6TaJ9RMZGXVEhBV9GP3zl0oJM2a+HnqeG48IxYF0cgaCqlreQHWBUXqFVJXS7wahdQbVkgkXSF//5HPx1zNp2MufW/2B7gIYvqNJNpH9WVCpJGIIZFB4nPfuSHdk2X5Aq7lGqpvIRNppYYe3+Pj+BNt0GU/+jg7ZS0+4he6egEsHULfgQ7R00dMAGtC+yFlltaCLdTSW0bLbLVbVssGddQT5sXZEBSpV09DLfFq1NNoWD3rr4kUZgyCk3Rv4WmaOxgFUzKa8Lk8wXhJkwfnu0HVShzUjXoTrfLUdnn64Mc0e/CGcYVNnvJlR8mkicjwzQ2wArP39Gc+sII6tq6O8rJuCorU666plng1ums2rLtGraF16XC3dN84Rm6Sv5a2SzhCz5U/k1VBakyMZjoHR/Te8YEJ6D5prk+IJu2oYkUvV3pTpPT0+73FobTp5NEqizrTP6K3+TOZCdW7EH+bh7dET2dyFqe0sJVsN/eWvUVfXyQGztdOzheSZp9lr6+y/EuTteK0QM0C92EwndC52uw27ZSE+Y30Y3ugv5jznfLpMT63usCiwMfgLgiZmU+7gL02yMFgkTOkDpgHtPgxKFcyBOzyc42954gxnQOAtqBIPaC11RKvBtDaDQOaKV6/EIRrb0op8WArZKo72Epvvx5stceZLPdgq02NHyOvW5agSP10mqWWeDXTaVbDs2mlSN96i51UzKYROfS8HsEPHB2yAsQKkghhxTk1sHNBKjii//xCX3x6Ko2BQZIis3RBAaNqOW4IZse4Vud0HyEN0pGdsv7k+vLmhkPKU99OGFD34/zbqI4pUFFD1M7BKVZvNXNwVsOmQLv+Ti15n+SCKQDLFh5iP6JRaDLO9bEXPG7ibljbjDFYS2MMIuxYElN4B7QDDT4ZU5AZtc8v5NNtQZF6S9xWS7waS7zhxRe6JVv9lgXt56GCeSQgmhDpc7zYpXm+d5l58WQe6er68pczuqE3T7iqo8T281Ti+ez4DWNF7M1WhAbfAUR0OKumDONcf+iyRLoxHrqOTzlpmjne7iZrLIgeIJOyEdlgUUScc1q0V0fd8+sL9I6gSL26d9QSr0bdG86F1e2m1P2M1B2MMZhhJ4xaSSYLU3/sxKOIrfEJAkD1eRCQUZpoAU3ioHq/eLbvhvTZXc+9w7FLauN6SePfTkSM0An93hrpMcXkWkrckjzzbAg0m4qWROjmb9aen7eWYMMp5dPRxyD0T/GA3MHDxEEQTxvoVL9YCM1ZZLUdzMlk3vXmCp7PjTU0QZFyBTc0tcQrUXBDa1jBO2JnOXPwS6WT45n08eNCAeEy1c/sjDodP9xg2KoZiOPKlom5FYNChWNhiofBrHEUjKEVdF/wqRcaezOhtrcqqLa52uczPw0oKFKv9lAt8WrUvuFMVkOTnXxzEYB+MJyBR+zej+LU1ASfu8c3X667n7sXt4lZvjv1ydgNFrM+ZChCpg5NsHtOVOGE3CHYgTpGW9+39qoP1kaNXNbl43YYPPaSZCGw1gTbklRxaGtaefVjwoGd0nic/fz7erqZz+w0kKBIvW4itcSr0c2GM1UN2FhiXD6DlXRFmkieBGafdqGXqyOSpY70IOsD7W0xFpwdtw4HpbGZNqZHvdb6S0pE0Wt5mwwhDSaB8yV383DxfqdDxIYjRTYNcN5eFr4z9BpQYeRzTg1dUKQeKnS1xKuBioZzaA35ObSdzg9psIcF2XAYEXebz3vj786ALsmmWR1l4746NOiSoIEfcf8kNmTWsjFk8HGPUE+VRRJAKFxzRqFm2bIzCHXBRgoCaCg0mKbYdDoHdawJI5/3ahiCIvUQYaglXg1ENJzHa+j1t/5cY92LEwMPk3oBBJnzEFrk5+l3vk4EXH+vE5o3jG2k3FtPL4pBlfb7fEdG9ANzwwUx9GDSief4/FjSCH+jUgmphE5GTsR0ID03lRYmTJo/xH7TUAotGRPYcSfEHxuM8ODPiCXY3YfYiXHIUzDpjhv0rSGm0y8M6HvMg6NvM6Yl2JVoCPnpZSk4zD5zyL49VyjCUtLyhDZaQAQ1X0Ce8PFj/olMAdP7hAGoCgPQkwxIYqqu746n42RjKSLeThpmLQSTfHxPePKAJbMIKWGRXoVF+pMsIuQ7dM1MHPCoPPfyE8Y536sxbhIQ10I+43QljDOqMM4oMc71CRsIOiyCHJJbayhprVmltebS1sazCVbXZFNJk9tVmtwuNZkLPh3w5qtYkoZHkpvdVtJsq0qzrVKzkzZyi4SOVI9h4N+Dh2Dg9KeeE84kN95S0ni7SuPt9Rv/3yB0BrJxzVbCgk4VFnQ2YQGOR747IJaLZD50lPABapWsJ20TThBLbcBGQuoou9IFY3FKlFyOVLMnoWAUIPZ2SD0WYkISMyrwgnvZQgDVWJCwkgkJyzbkKXNlqGHjgJuLy8/dUzZ7IbvVaoxCWMkqhGWzkJ52m7b7+vtFEI7TwU92w9UYdbCSVQfLZt38mO+09SdXty1wdnLaoXfoLpI/nVyd3MjmghpjD1ay9qC1HUVXM/LDSuYeLI/9NC9mnmBEez/x1O+mMREH2Y1XM+bDSkYf7Dzd+EnoElmnU5myW65olK9k8aHyKE9DXDxQJbulSNHoXcmwQ1DYUppfN3LvR7KbqijsU8l0Q4LAT+DHI28G4hD7sp1UpGaoRpWMMqQ321Y1ozOqFs8rj86p+IKhM4t60XQy8WQPS0jNUIwqWWLIXN7kRQpsiO9cT3YQAqkJvqBKhhgqh1/cMXGupnwD8G9Tx49ppJxGsIfENJXddjURGJQ3v/hcQ7HllkCh0y0SYHaadh6FCoPHCOymQTiN/d5bypF+EI8KnOCEJE0XW2GcrFzjE0qzzZ8/lSvKsaBdgQVlW0zFYR0bc8iuwyG7zCE7zyGrAoc6Agud7jKd2WHaiUGEfWq0pak+9KQ+U5ttNMOTY4DYYps3rViUZ0CnzIBOngH20wzQy9ZMSBuTXyMUB/m9MqNNu1yHlVvcKbWY05prcbrzatriToUWl42aDfcirNv9utjemTezWJRnBiozA+XnrbQKzChbPbL2gavNHb0yd1CZO3qZO3qeO7ACd8p20lMbcBHX1gFDtgmVAoExKrNEL7PEKLPEyLMEVWCJuZwl625LpIBBZmUGGWUGmWUGmXkG6RUY1F7pHqLaTWxXbqJZbmK73MR2volGhSaWLavyMvCNRwircvva5faVzSY9bzbpFSxH3RYFL9lQCG6SpZ6zao2t2qliS0gvW0J62RLSy5aQnreE9Aq2ol62hG5TB4mui6M2QLo2DrDFcRTrFqsOa7OgU5kFZVtIL9tCet4W0isYg0Y5hlVtZVbdthta5baXrSJOda7t6eK9tO0V7EADCnT6yfUotRsOqzY86Ztcw8vmoJE3B/UK5qDxlDkozuxPltPWZgCqzICyCWiUTUAjbwIaFUxAo2wCbp7GXJsfemV+lI0+o2z0GXmjz6hg9BnGWv7BpjmbtTllVOZU2RY0yrYgzQf+4z//D7YGi/njTwEA"""
pack = json.loads(gzip.decompress(base64.b64decode(PAYLOAD)).decode())
TEMPLATE_REPLACEMENTS = {
    "dq4_omop_20260825_r5": RUN,
    "dq4_omop_20260825_r2": RUN,
    "2026-08-26T06:48:21.102Z": RUN_OPEN_TS,
    "cc682c9c-8795-4c48-adea-f988320f8d0d": SOURCE_UPDATE_ID,
    "f5c7c7ab-e37d-4a31-b9c2-b7631becb16a": SILVER_UPDATE_ID,
    "r5q9n3k6": SCRATCH_PREFIX,
}
def adapt(value):
    if isinstance(value, str):
        for old, new in TEMPLATE_REPLACEMENTS.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [adapt(v) for v in value]
    if isinstance(value, dict):
        return {k: adapt(v) for k, v in value.items()}
    return value
pack = adapt(pack)

def qs(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

def execute(seq, name, sql):
    sha = hashlib.sha256(sql.encode()).hexdigest()
    spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
      ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'attempted',
       NULL,NULL,current_timestamp(),NULL,{qs(SESSION)})""")
    try:
        spark.sql(sql).collect()
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'ok',
           NULL,NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
    except Exception as exc:
        msg = str(exc)[:4000]
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'error',
           {qs(msg)},NULL,current_timestamp(),current_timestamp(),{qs(SESSION)})""")
        raise

# A retry may reuse this notebook only after clearing its own scratch namespace.
old = [r.table_name for r in spark.sql(f"""
SELECT table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema='silver_qc_tmp' AND table_name LIKE {qs(PREFIX + '%')}
""").collect()]
for i, name in enumerate(sorted(old)):
    execute(1000+i, "preclean:" + name,
            "DROP TABLE IF EXISTS 6_mgmt.silver_qc_tmp." + chr(96) + name + chr(96))

seq = 0
for item in pack["parallel"]:
    execute(seq, item["file"], item["sql"])
    seq += 1

names = [r.table_name for r in spark.sql(f"""
SELECT table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema='silver_qc_tmp' AND table_name LIKE {qs(PREFIX + '%')}
""").collect()]
rd = sorted((n for n in names if re.fullmatch(re.escape(PREFIX) + r"[0-9]+", n)),
            key=lambda n:int(n.rsplit("_",1)[1]))
hr = sorted((n for n in names if re.fullmatch(re.escape(PREFIX) + r"rule_[0-9]+", n)),
            key=lambda n:int(n.rsplit("_",1)[1]))
assert rd and hr, (len(rd),len(hr))
rd_select = """SELECT CAST(analysis_id AS INT) analysis_id,
 CAST(stratum_1 AS STRING) stratum_1,CAST(stratum_2 AS STRING) stratum_2,
 CAST(statistic_value AS DOUBLE) statistic_value,CAST(measure_id AS STRING) measure_id
 FROM 6_mgmt.silver_qc_tmp.{}"""
hr_select = """SELECT CAST(analysis_id AS INT) analysis_id,
 CAST(ACHILLES_HEEL_warning AS STRING) ACHILLES_HEEL_warning,
 CAST(rule_id AS INT) rule_id,CAST(record_count AS BIGINT) record_count
 FROM 6_mgmt.silver_qc_tmp.{}"""
execute(20000, "phase2_achilles_rd_0.sql",
        f"CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.{PREFIX}achilles_rd_0 AS " +
        " UNION ALL ".join(rd_select.format(n) for n in rd))
execute(20001, "phase2_achilles_hr_0.sql",
        f"CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.{PREFIX}achilles_hr_0 AS " +
        " UNION ALL ".join(hr_select.format(n) for n in hr))

for item in pack["serial"]:
    execute(30000+seq, item["file"], item["sql"])
    seq += 1

names = [r.table_name for r in spark.sql(f"""
SELECT table_name FROM 6_mgmt.information_schema.tables
WHERE table_schema='silver_qc_tmp' AND table_name LIKE {qs(PREFIX + 'serial_%')}
""").collect()]
hrs = [(int(re.fullmatch(re.escape(PREFIX)+r"serial_hr_(\d+)",n).group(1)),n)
       for n in names if re.fullmatch(re.escape(PREFIX)+r"serial_hr_(\d+)",n)]
rds = [(int(re.fullmatch(re.escape(PREFIX)+r"serial_rd_(\d+)",n).group(1)),n)
       for n in names if re.fullmatch(re.escape(PREFIX)+r"serial_rd_(\d+)",n)]
assert hrs and rds
hr_final=max(hrs)[1]
rd_final=max(rds)[1]
execute(40000, "phase4_achilles_heel_results.sql",
        f"CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.{PREFIX}achilles_heel_results AS SELECT * FROM 6_mgmt.silver_qc_tmp.{hr_final}")
execute(40001, "phase4_achilles_results_derived.sql",
        f"CREATE OR REPLACE TABLE 6_mgmt.silver_qc_tmp.{PREFIX}achilles_results_derived AS SELECT * FROM 6_mgmt.silver_qc_tmp.{rd_final}")

assert RUN.startswith("dq4_omop_")
for j, table in enumerate(("heel_results","heel_results_derived","heel_rule_disposition")):
    prior=spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.{table} WHERE run_id={qs(RUN)}").first().n
    assert prior>=0
    execute(41000+j,f"delete_{table}.sql",
            f"DELETE FROM 6_mgmt.silver_qc.{table} WHERE run_id={qs(RUN)}")
execute(41010,"append_heel_results.sql",
        f"INSERT INTO 6_mgmt.silver_qc.heel_results SELECT {qs(RUN)},* FROM 6_mgmt.silver_qc_tmp.{PREFIX}achilles_heel_results")
execute(41011,"append_heel_results_derived.sql",
        f"INSERT INTO 6_mgmt.silver_qc.heel_results_derived SELECT {qs(RUN)},* FROM 6_mgmt.silver_qc_tmp.{PREFIX}achilles_results_derived")

vals=",\n".join("(" + ",".join(qs(r[k]) for k in ("rule_id","rule_name","phase","sql_rel")) + ")"
                 for r in pack["plan"])
disp=f"""INSERT INTO 6_mgmt.silver_qc.heel_rule_disposition
(run_id,rule_id,rule_name,phase,sql_file,disposition,reason,result_rows,created_by_session,created_at)
WITH rules(rule_id,rule_name,phase,sql_file) AS (SELECT * FROM VALUES {vals}),
hits AS (SELECT CAST(rule_id AS STRING) rule_id,count(*) n
         FROM 6_mgmt.silver_qc.heel_results WHERE run_id={qs(RUN)} GROUP BY rule_id)
SELECT {qs(RUN)},r.rule_id,r.rule_name,r.phase,r.sql_file,
       CASE WHEN coalesce(h.n,0)>0 THEN 'lifted' ELSE 'skipped_unpopulated' END,
       CASE WHEN coalesce(h.n,0)>0 THEN 'executed; rule output rows emitted'
            ELSE 'executed; no warning rows emitted under post-O4 populated-table scope' END,
       coalesce(h.n,0),'DQ4',current_timestamp()
FROM rules r LEFT JOIN hits h ON h.rule_id=r.rule_id"""
execute(41012,"insert_rule_disposition.sql",disp)

summary={
 "parallel_statements":len(pack["parallel"]),
 "serial_statements":len(pack["serial"]),
 "parallel_rd_tables":len(rd),
 "parallel_hr_tables":len(hr),
 "heel_rows":spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.heel_results WHERE run_id={qs(RUN)}").first().n,
 "derived_rows":spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.heel_results_derived WHERE run_id={qs(RUN)}").first().n,
 "lifted":spark.sql(f"SELECT count(*) n FROM 6_mgmt.silver_qc.heel_rule_disposition WHERE run_id={qs(RUN)} AND disposition='lifted'").first().n
}
print(summary)


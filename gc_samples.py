"""Finite interleavings and raw probes; no protection protocol."""
import copy
from samples import FILES, LAYOUT


def action(ident, op, **fields):
    return dict(id=ident, op=op, **fields)


def prepare(ident, job):
    return action(ident, "prepare", job=job, inputs=["a", "b"], layout=LAYOUT)


def cases():
    seed = action("seed", "seed", files=FILES)
    reader = action("reader", "read", snapshot=1, at={
        "point": "before_get", "value": "b", "actions": [
            prepare("p", "r"), action("publish", "publish", job="r"),
            action("expire", "expire", snapshot=1),
            action("during", "collect")]})
    partial = prepare("p", "p")
    partial["at"] = {"point": "after_put", "value": "rewrite-p-0",
                     "actions": [action("partial", "collect")]}
    return copy.deepcopy({
        "reader": [seed, reader, action("after", "collect"),
                   action("late", "read", snapshot=1)],
        "prepared": [seed, partial,
                     action("delete", "delete", snapshot=1, file="a", pos=0),
                     action("expire", "expire", snapshot=1),
                     action("conflict", "publish", job="p"),
                     action("held", "collect"),
                     action("cancel", "cancel", job="p"),
                     action("released", "collect")],
        "rollback": [seed, prepare("p", "p"),
                     action("tx", "rollback", actions=[
                         action("publish_inside", "publish", job="p"),
                         action("expire_inside", "expire", snapshot=1),
                         action("collect_inside", "collect"),
                         action("read_inside", "read")]),
                     action("after_rollback", "collect"),
                     action("publish", "publish", job="p"),
                     action("expire", "expire", snapshot=1),
                     action("after_commit", "collect"),
                     action("current", "read")],
        "cancel_name": [seed, prepare("p", "c"),
                        action("cancel", "cancel", job="c"),
                        action("collect", "collect"),
                        action("raw_remove", "raw_drop", file="rewrite-c-0"),
                        action("raw_reuse", "raw_put", file="rewrite-c-0",
                               rows=[{"reading": 999}])],
        "shared_empty": [action("seed", "seed", files=dict(FILES, empty=[])),
                         action("delete", "delete", snapshot=1, file="a", pos=0),
                         action("prepare", "prepare", job="e", inputs=["empty"], layout=[]),
                         action("publish", "publish", job="e"),
                         action("expire_old", "expire", snapshot=1),
                         action("shared", "collect"),
                         action("expire_middle", "expire", snapshot=2),
                         action("empty", "collect"),
                         action("current", "read")]
    })

"""Raw storage observations, not an expiry or garbage-collection implementation."""
import copy
import json
import sys
from catalog import Catalog
from lake import Lake
from objects import Objects
from gc_samples import cases


class ProbeObjects(Objects):
    def __init__(self, emit, root=None):
        super().__init__(root)
        self.emit = emit

    def inventory(self):
        if self.root:
            return sorted(p.stem for p in self.root.glob("*.json"))
        return sorted(self.memory)

    def drop_raw(self, fid):
        # Deliberately no reachability or lifetime checks.
        if self.root:
            (self.root / (fid + ".json")).unlink()
        else:
            del self.memory[fid]
        self.emit("drop", fid)

    def get(self, fid):
        self.emit("before_get", fid)
        value = super().get(fid)
        self.emit("after_get", fid)
        return value

    def put(self, fid, rows):
        self.emit("before_put", fid)
        super().put(fid, rows)
        self.emit("after_put", fid)


class ProbeCatalog(Catalog):
    def __init__(self, emit):
        super().__init__()
        self.emit = emit
        self.db.set_trace_callback(self.trace)

    def trace(self, sql):
        if sql in ("BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"):
            self.emit("sql", sql)

    def state(self, sid=None):
        value = super().state(sid)
        self.emit("after_state", value[0])
        return value

    def commit(self, files, dead):
        value = super().commit(files, dead)
        self.emit("after_commit", value)
        return value


class Observer:
    def __init__(self):
        self.events = []
        self.hooks = []
        self.plans = {}
        self.catalog = ProbeCatalog(self.emit)
        self.objects = ProbeObjects(self.emit)
        self.lake = Lake(self.catalog, self.objects)

    def emit(self, point, value):
        self.events.append({"event": point, "value": value,
                            "in_transaction": self.catalog.db.in_transaction})
        for hook in list(self.hooks):
            if hook["point"] == point and ("value" not in hook or hook["value"] == value):
                self.hooks.remove(hook)
                for child in hook["actions"]:
                    self.act(child)

    def act(self, action):
        ident, op = action["id"], action["op"]
        self.events.append({"event": "begin", "id": ident, "op": op})
        hook = copy.deepcopy(action.get("at"))
        if hook:
            self.hooks.append(hook)
        result = None
        try:
            if op == "seed":
                result = self.lake.seed(action["files"])
            elif op == "read":
                result = self.lake.read(action.get("snapshot"))
            elif op in ("prepare", "rewrite"):
                result = getattr(self.lake, op)(
                    action["job"], action["inputs"], action["layout"])
                if op == "prepare":
                    self.plans[action["job"]] = result
            elif op == "publish":
                result = self.lake.publish(self.plans[action["job"]])
            elif op == "delete":
                result = self.lake.delete(action["snapshot"], action["file"], action["pos"])
            elif op in ("expire", "cancel", "collect"):
                # Record only. These operations do not exist in the baseline.
                result = {"request_only": copy.deepcopy(action)}
            elif op == "rollback":
                try:
                    with self.catalog.write():
                        for child in action["actions"]:
                            self.act(child)
                        raise RuntimeError("OBSERVER_ROLLBACK")
                except RuntimeError as exc:
                    if str(exc) != "OBSERVER_ROLLBACK":
                        raise
                result = "ROLLED_BACK"
            elif op == "raw_drop":
                self.objects.drop_raw(action["file"])
            elif op == "raw_put":
                self.objects.put(action["file"], action["rows"])
            else:
                raise ValueError("BAD_OPERATION")
            record = {"event": "end", "id": ident, "op": op, "result": result}
        except (ValueError, KeyError, FileNotFoundError) as exc:
            record = {"event": "end", "id": ident, "op": op,
                      "error": str(exc)}
        finally:
            if hook in self.hooks:
                self.hooks.remove(hook)
                raise AssertionError("HOOK_NOT_REACHED:" + ident)
        record.update(head=self.catalog.head(), objects=self.objects.inventory(),
                      in_transaction=self.catalog.db.in_transaction)
        self.events.append(record)
        return record


def run(spec):
    observer = Observer()
    try:
        for action in spec:
            observer.act(copy.deepcopy(action))
        return observer.events
    finally:
        observer.catalog.db.close()


def main(argv):
    if len(argv) == 2 and argv[0] == "--input":
        with open(argv[1], encoding="utf-8") as stream:
            selected = json.load(stream)
    elif len(argv) == 1:
        selected = cases()
        if argv[0] != "all":
            selected = {argv[0]: selected[argv[0]]}
    else:
        raise SystemExit("usage: gc_observe.py all|CASE OR --input FILE")
    for name, spec in selected.items():
        print(json.dumps({"case": name, "actual": run(spec)}, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])

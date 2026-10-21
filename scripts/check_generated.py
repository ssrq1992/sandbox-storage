"""Re-generate both internal protocols in a temporary directory and compare."""
import pathlib
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parents[1]
for source, proto_path in [
    (root, "api/ssf/v1/node.proto"),
    (root.parent / "AgentENV/services", "api/proto/scheduler.proto"),
]:
    with tempfile.TemporaryDirectory(prefix="ssf-protocol-check-") as output:
        subprocess.run([
            "go", "run", "./tools/generate", "-root", str(source),
            "-proto", proto_path, "-out", output,
        ], cwd=root, check=True)
        generated = list(pathlib.Path(output).rglob("*.pb.go"))
        if len(generated) != 2:
            raise SystemExit(f"expected protobuf and grpc bindings for {proto_path}")
        for file in generated:
            checked_in = source / file.relative_to(output)
            if not checked_in.exists() or file.read_bytes() != checked_in.read_bytes():
                raise SystemExit(f"generated protobuf file is stale: {checked_in}")
print("SSF and AgentENV protobuf generation is reproducible")

import Lean

open Lean

private def proofSentinel : Expr := .const `_paf.proofErased []

private def sanitizeConstant : ConstantInfo → ConstantInfo
  | .thmInfo value => .thmInfo { value with value := proofSentinel }
  | .opaqueInfo value => .opaqueInfo { value with value := proofSentinel }
  | value => value

private def isInterfaceEntry (name : Name) : Bool :=
  let value := name.toString
  value != "Lean.declRangeExt" &&
    !value.endsWith ".Lean.extraModUses" &&
    !value.startsWith "Lean.IR." &&
    !value.startsWith "Lean.Compiler."

private def constantRefs (info : ConstantInfo) : Array Name :=
  let refs := info.type.foldConsts #[] fun name values => values.push name
  let refs := match info with
    | .defnInfo value => value.value.foldConsts refs fun name values => values.push name
    | .inductInfo value => refs ++ value.all.toArray ++ value.ctors.toArray
    | .ctorInfo value => refs.push value.induct
    | .recInfo value =>
      value.rules.foldl (init := refs ++ value.all.toArray) fun values rule =>
        rule.rhs.foldConsts (values.push rule.ctor) fun name result => result.push name
    | _ => refs
  refs

private def exportedConstants (constants : Array ConstantInfo) : Array ConstantInfo := Id.run do
  let available : Std.HashMap Name ConstantInfo :=
    constants.foldl (init := {}) fun values info => values.insert info.name info
  let mut reachable : Std.HashSet Name := {}
  let mut pending : Array Name := #[]
  for info in constants do
    if !isPrivateName info.name then
      reachable := reachable.insert info.name
      pending := pending.push info.name
  let mut cursor := 0
  while cursor < pending.size do
    let name := pending[cursor]!
    cursor := cursor + 1
    if let some info := available[name]? then
      for referenced in constantRefs info do
        if available.contains referenced && !reachable.contains referenced then
          reachable := reachable.insert referenced
          pending := pending.push referenced
  return constants.filter fun info => reachable.contains info.name

private def sanitize (data : ModuleData) : ModuleData :=
  let constants := exportedConstants <| data.constants.map sanitizeConstant
  { data with
    constNames := constants.map ConstantInfo.name
    constants
    -- These names identify code-generator implementation details and are not
    -- part of the environment imported by downstream Lean modules.
    extraConstNames := #[]
    -- Source ranges and module-usage telemetry vary with proof scripts but do
    -- not participate in downstream elaboration or type checking.
    entries := data.entries.filter fun item => isInterfaceEntry item.1 }

private def resultJson
    (moduleName : String) (data : ModuleData) (output : System.FilePath) : Json :=
  Json.mkObj [
    ("module", toJson moduleName),
    ("imports", toJson <| data.imports.map fun item => item.module.toString),
    ("declaration_count", toJson data.constants.size),
    ("lean_version", toJson Lean.versionString),
    ("lean_githash", toJson Lean.githash),
    ("sanitized_path", toJson output.toString)
  ]

private def process (moduleName input output : String) : IO Json := do
  let (data, _region) ← Lean.readModuleData input
  let sanitized := sanitize data
  Lean.saveModuleData output moduleName.toName sanitized
  return resultJson moduleName sanitized output

def main (args : List String) : IO UInt32 := do
  if args.length % 3 != 0 then
    IO.eprintln "usage: InterfaceFingerprint <module> <input.olean> <output.olean> [...]"
    return 2
  let mut results : Array Json := #[]
  let mut remaining := args
  while !remaining.isEmpty do
    match remaining with
    | moduleName :: input :: output :: tail =>
      results := results.push (← process moduleName input output)
      remaining := tail
    | _ => unreachable!
  IO.println (Json.arr results).compress
  return 0

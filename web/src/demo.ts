import type { LeanStatement, StatementResponse, SwarmState, Task } from "./types";

const now = new Date().toISOString();
const chapters = [
  ["book03", 8, "Discriminants"],
  ["book04", 11, "Ray class groups"],
  ["book07", 6, "The explicit formula"],
  ["book08", 4, "Symmetric powers"],
  ["book09", 9, "Cohomology and base change"],
  ["book10", 5, "Faithfully flat descent"],
] as const;

const demoTasks: Record<string, Task> = {};
chapters.forEach(([book, chapter, title], chapterIndex) => {
  (["formalize", "fixup", "review", "prove"] as const).forEach((stage, stageIndex) => {
    const statuses = ["succeeded", "succeeded", "running", "pending"] as const;
    const status =
      chapterIndex === 0 ? statuses[stageIndex] :
      chapterIndex === 1 && stageIndex < 2 ? "succeeded" :
      chapterIndex === 2 && stageIndex === 0 ? "running" : "pending";
    demoTasks[`${book}/chapter-${String(chapter).padStart(2, "0")}:${stage}`] = {
      chapter_id: `${book}/chapter-${String(chapter).padStart(2, "0")}`,
      book_id: book,
      chapter_number: chapter,
      chapter_title: title,
      stage,
      status,
      phase: status === "running" ? "agent" : "idle",
      detail:
        status === "running"
          ? stage === "review"
            ? "checking declaration boundaries and imports"
            : "formalizing §6.3 prime and discriminant terms"
          : "",
      rounds: status === "succeeded" ? 1 : 0,
      updated_at: now,
      latest_run_id: status === "running" ? `demo-${chapterIndex}` : null,
    };
  });
});

export const demoState: SwarmState = {
  source: "demo snapshot",
  updated_at: now,
  usage: {
    input_tokens: 39_781_514,
    cached_input_tokens: 34_990_080,
    output_tokens: 1_284_901,
    reasoning_output_tokens: 817_551,
    total_tokens: 41_883_966,
  },
  invocation_usage: {
    input_tokens: 4_822_114,
    cached_input_tokens: 3_771_008,
    output_tokens: 218_332,
    reasoning_output_tokens: 102_881,
    total_tokens: 5_143_327,
  },
  cost: { estimated_usd: 128.42 },
  invocation_cost: { estimated_usd: 14.08 },
  agents: { active: 2, maximum: 30, queued: 4, by_stage: { formalize: 1, review: 1 } },
  scheduling: { statements: { critical_path: ["book10", "book17", "book29"] } },
  isolation: { backend: "fuse-overlay" },
  coordinator_build: {
    active: true,
    mode: "targeted",
    stage: "review",
    completed: 143,
    total: 216,
    iteration: 1,
    maximum_iterations: 4,
    error_count: 0,
    warning_count: 2,
    current_chapter_id: "book03/chapter-08",
    target_chapter_ids: ["book03/chapter-08"],
    output_tail: [
      "Building LastLib.Book03RamificationTheory.Chapter08",
      "[143/216] Section02PolynomialDiscriminantsAndTheIndexCorrection",
    ],
  },
  tasks: demoTasks,
  activities: {
    "demo-0": {
      current: "Lean diagnostics · Section02PolynomialDiscriminants…:59",
      updated_at: now,
      commands: 7,
      mcp_calls: 18,
      file_changes: 3,
      failures: 0,
      todo_completed: 2,
      todo_total: 4,
      todos: [
        { completed: true, text: "Inventory the chapter and existing Lean declarations" },
        { completed: true, text: "Identify the matching Mathlib discriminant API" },
        { completed: false, text: "Close the remaining minpoly identity" },
        { completed: false, text: "Run diagnostics and audit chapter coverage" },
      ],
      latest_summary: "Closed two proof holes; checking the remaining minpoly identity.",
      recent: [
        { at: now, kind: "mcp_tool_call", status: "completed", title: "Lean diagnostics", detail: "0 errors · 1 declaration uses sorry" },
        { at: now, kind: "file_change", status: "completed", title: "Edited discriminant formula", detail: "Section02PolynomialDiscriminantsAndTheIndexCorrection.lean" },
        { at: now, kind: "reasoning", status: "updated", title: "Comparing power-basis normalizations", detail: "The Mathlib theorem uses the same sign convention." },
      ],
    },
    "demo-2": {
      current: "formalizing prime and discriminant terms",
      updated_at: now,
      commands: 2,
      mcp_calls: 0,
      file_changes: 1,
      failures: 0,
      todo_completed: 1,
      todo_total: 4,
      todos: [
        { completed: true, text: "Inventory the source section and scoped files" },
        { completed: false, text: "State the finite-prime contribution" },
        { completed: false, text: "Formalize the discriminant term" },
        { completed: false, text: "Audit coverage and unresolved dependencies" },
      ],
      recent: [
        { at: now, kind: "file_change", status: "completed", title: "Created Section03PrimeAndDiscriminantTerms.lean" },
        { at: now, kind: "todo", status: "updated", title: "Plan 3/6", detail: "State the finite-prime contribution." },
      ],
    },
  },
};

const sampleStatements: LeanStatement[] = [
  {
    id: "demo-discr",
    name: "chapter08_power_basis_discriminant_eq_norm_derivative",
    kind: "theorem",
    signature: `theorem chapter08_power_basis_discriminant_eq_norm_derivative
    (K L : Type*) [Field K] [Field L] [Algebra K L]
    [FiniteDimensional K L] [Algebra.IsSeparable K L]
    (pb : PowerBasis K L) :
    chapter08PowerBasisDiscriminant K L pb =
      (chapter08DiscriminantSign pb.dim : K) *
        Algebra.norm K (aeval pb.gen (minpoly K pb.gen).derivative) := …`,
    excerpt: `/-- The discriminant of a power basis is its signed norm of the derivative. -/
theorem chapter08_power_basis_discriminant_eq_norm_derivative
    (K L : Type*) [Field K] [Field L] [Algebra K L]
    [FiniteDimensional K L] [Algebra.IsSeparable K L]
    (pb : PowerBasis K L) :
    chapter08PowerBasisDiscriminant K L pb =
      (chapter08DiscriminantSign pb.dim : K) *
        Algebra.norm K
          (aeval pb.gen (minpoly K pb.gen).derivative) := by
  rw [← pb.finrank]
  simpa [chapter08PowerBasisDiscriminant, chapter08DiscriminantSign] using
    (Algebra.discr_powerBasis_eq_norm K pb)`,
    doc: "The discriminant of a power basis is its signed norm of the derivative.",
    path: "lean/LastLib/Book03RamificationTheory/Chapter08/Section02PolynomialDiscriminantsAndTheIndexCorrection.lean",
    line: 47,
    endLine: 58,
    book: "Ramification Theory",
    bookNumber: 3,
    chapter: 8,
    section: "2. Polynomial Discriminants And The Index Correction",
    status: "proved",
  },
  {
    id: "demo-adjoin",
    name: "chapter08MonogenicIntegralExtension_iff",
    kind: "theorem",
    signature: `theorem chapter08MonogenicIntegralExtension_iff
    (A B K L : Type*) [CommRing A] [CommRing B]
    (α : B) : chapter08MonogenicIntegralExtension A B K L α ↔
      IsIntegral A α ∧ Algebra.adjoin A ({α} : Set B) = ⊤ := …`,
    excerpt: `@[simp]
theorem chapter08MonogenicIntegralExtension_iff
    (A B K L : Type*) [CommRing A] [CommRing B] [Field K] [Field L]
    [Algebra A B] [Algebra A K] [Algebra K L] [Algebra B L]
    (α : B) :
    chapter08MonogenicIntegralExtension A B K L α ↔
      IsIntegral A α ∧
        Algebra.adjoin A ({α} : Set B) = ⊤ ∧
        Algebra.adjoin K ({algebraMap B L α} : Set L) = ⊤ := Iff.rfl`,
    doc: "The concrete characterization of a monogenic integral extension.",
    path: "lean/LastLib/Book03RamificationTheory/Chapter08/Section02PolynomialDiscriminantsAndTheIndexCorrection.lean",
    line: 90,
    endLine: 101,
    book: "Ramification Theory",
    bookNumber: 3,
    chapter: 8,
    section: "2. Polynomial Discriminants And The Index Correction",
    status: "proved",
  },
  {
    id: "demo-hilbert",
    name: "chapter09_hilbert_different_formula",
    kind: "theorem",
    signature: `theorem chapter09_hilbert_different_formula
    (L K : Type*) [Field K] [Field L] [Algebra K L] :
    differentExponent L K = ∑ σ, lowerRamificationIndex σ := …`,
    excerpt: `theorem chapter09_hilbert_different_formula
    (L K : Type*) [Field K] [Field L] [Algebra K L]
    [FiniteDimensional K L] [IsGalois K L] :
    chapter09DifferentExponent K L =
      ∑ σ : L ≃ₐ[K] L, chapter09LowerRamificationIndex σ := by
  sorry`,
    doc: "Hilbert's formula expresses the different exponent as total ramification displacement.",
    path: "lean/LastLib/Book03RamificationTheory/Chapter09/Section01TheFormulaAndItsHypotheses.lean",
    line: 34,
    endLine: 40,
    book: "Ramification Theory",
    bookNumber: 3,
    chapter: 9,
    section: "1. The Formula And Its Hypotheses",
    status: "sorry",
  },
];

export const demoStatementResponse: StatementResponse = {
  source: "demo",
  total: sampleStatements.length,
  declarations: sampleStatements,
  facets: {
    books: [{ id: "3", number: 3, label: "Ramification Theory", count: 3 }],
    kinds: { theorem: 3 },
    statuses: { proved: 2, sorry: 1 },
  },
};

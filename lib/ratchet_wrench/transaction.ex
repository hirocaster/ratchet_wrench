defmodule RatchetWrench.Transaction do
  defstruct id: nil, seqno: 1, session: nil, transaction: nil, skip: 0, rollback: false
end

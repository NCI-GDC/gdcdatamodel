import os
import subprocess

import signpostclient as signpost

from validator import settings

def validate():
  # Get the current state.
  state = signpost.find(settings['state'])

  # Generate a set of claimed DIDs.
  claim = set(state['meta']['claimed'].keys())

  # Get the current list of candidates.
  candi = set(signpost.find(settings['query'])['dids'])

  # Find candidates not yet claimed.
  candi = claim - candi

  # Select a candidate.
  candi = next(iter(candi))

  # Update local state with a claim.
  state['meta']['claimed'][candi] = 'tmp_id'

  # Attempt to claim a candidate.
  ret = signpost.post(state['did'],state['rev'],state['meta'],meta=True)
  if 'error' in ret: return # Early return - we failed to claim a candidate.

  # Pull selected candidate state.
  candi_state = signpost.find(candi)

  # Pull data from candidate data did.
  filename = signpost.find(candi_state['meta']['data_did'])['files'][0]

  # Validate data as bam format.
  valid = bool(subprocess.call(['picard-tools','ValidateSamFile',filename]))

  # Delete the file.
  os.remove(filename)

  # Update state with results of validation.
  while True:

    # Refresh state.
    state = signpost.find(settings['state'])

    # Update local state with result.
    state['meta']['valid' if valid else 'invalid'].append(candi)

    # Attempt to push state.
    ret = signpost.post(state['did'],state['rev'],state['meta'],meta=True)
    if 'error' in ret: continue # Try again until success.

    # Successfully updated state - we're done.
    return

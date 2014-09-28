import os
import json
import random
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
  candi = candi - claim

  # If non-null set, we have something to look at.
  if not candi: return

  # Select a candidate.
  candi = random.sample(candi,1)[0]

  # Update local state with a claim.
  state['meta']['claimed'][candi] = 'tmp_id'

  # Attempt to claim a candidate.
  ret = signpost.post(state['did'],state['rev'],json.dumps(state['meta']),meta=True)
  if 'error' in ret: return # Early return - we failed to claim a candidate.

  print 'claimed %s' % candi

  # Attempt to pull down the candidate information.
  try:

    # Pull selected candidate state.
    candi_state = signpost.find(candi)

    # Pull data from candidate data did.
    filename = signpost.find(candi_state['meta']['data_did'])['files'][0]

  # Failed to pull the data, unclaim the candidate and exit gracefully.
  except Exception as err:

    print err
    print 'unclaiming %s' % candi

    # Keep trying to unclaim the candidate.
    while True:

      state = signpost.find(settings['state'])

      del state['meta']['claimed'][candi]

      ret = signpost.post(state['did'],state['rev'],json.dumps(state['meta']),meta=True)
      if 'error' in ret: continue # Try again until success

      print 'unclaimed %s' % candi

      return

  print 'pulled down %s' % filename

  # Validate data as bam format.
  try: valid = subprocess.check_output([
         'picard-tools',
         'ValidateSamFile',
         'INPUT=%s' % filename,
       ],stderr=subprocess.STDOUT,)
  except Exception as err:
    print 'file not valid bam format'
    print err
    return
  else: print 'file validated as bam format'
  finally: os.remove(filename)

  # Update state with results of validation.
  while True:

    print 'attempting to post state'

    output = signpost.init()

    meta = {
      '_type' : 'cghub_import_validation',
      'cghub_import_did' : state['did'],
      'picard_output' : valid,
    }

    ret = signpost.post(output['did'],output['rev'],json.dumps(meta),meta=True)
    if 'error' in ret: continue # Try again until success.

    print 'posted state'

    # Successfully updated state - we're done.
    return

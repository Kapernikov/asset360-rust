import { ready as readyAsset360 } from 'asset360-rust';
import * as asset360 from 'asset360-rust';
const { expect } = require('chai');

const SCHEMA_YAML = `
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Person:
    slots:
      - name
      - aliases
      - role
slots:
  name:
    range: string
  aliases:
    range: string
    multivalued: true
  role:
    range: PersonRole
enums:
  PersonRole:
    permissible_values:
      manager: {}
`;

const PERSON_JSON = JSON.stringify({ name: 'Alice', aliases: ['Al'], role: 'manager' });

const IDENTITY_SCHEMA_YAML = `
id: https://example.org/identity
name: identity
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Service:
    slots:
      - name
      - sections
      - contacts
      - notes
      - pictures
      - cover
  Section:
    slots:
      - sequenceNumber
      - note
    unique_keys:
      seq:
        unique_key_slots:
          - sequenceNumber
  Contact:
    slots:
      - kind
      - primary
      - phone
    unique_keys:
      ck:
        unique_key_slots:
          - kind
          - primary
  Note:
    slots:
      - body
  Picture:
    slots:
      - sequence
      - caption
slots:
  name:
    range: string
  sections:
    range: Section
    multivalued: true
    inlined_as_list: true
  contacts:
    range: Contact
    multivalued: true
    inlined_as_list: true
  notes:
    range: Note
    multivalued: true
    inlined_as_list: true
  sequenceNumber:
    range: integer
  note:
    range: string
  kind:
    range: string
  primary:
    range: boolean
  phone:
    range: string
  body:
    range: string
  pictures:
    range: Picture
    multivalued: true
    inlined_as_list: true
  sequence:
    range: integer
    key: true
  caption:
    range: string
  cover:
    range: Picture
    inlined: true
`;

/** Path segments `diff()` actually emitted under `slot`, in emission order. */
function diffSegmentsUnder(deltas: Array<{ path: string[] }>, slot: string): string[] {
  const seen: string[] = [];
  for (const delta of deltas) {
    if (delta.path[0] === slot && delta.path.length > 1 && !seen.includes(delta.path[1])) {
      seen.push(delta.path[1]);
    }
  }
  return seen;
}

describe('LinkMLInstance wasm bindings', () => {
  before(async () => {
    await readyAsset360();
  });

  it('creates and inspects instances via wasm', () => {
    const view = asset360.loadSchemaView(SCHEMA_YAML);
    const instance = view.loadInstanceFromJson('Person', PERSON_JSON);

    expect(instance.kind()).to.equal('object');
    expect(instance.className()).to.equal('Person');

    const keys = instance.keys().sort();
    expect(keys).to.deep.equal(['aliases', 'name', 'role']);

    const aliasList = instance.get('aliases');
    expect(aliasList, 'aliases slot').to.not.be.undefined;
    expect(aliasList!.kind()).to.equal('list');
    expect(aliasList!.length()).to.equal(1);

    const aliasZero = aliasList!.at(0);
    expect(aliasZero, 'first alias').to.not.be.undefined;
    expect(aliasZero!.scalarValue()).to.equal('Al');

    const classViewHandle = instance.classView();
    expect(classViewHandle).to.not.be.undefined;
    expect(classViewHandle!.name()).to.equal('Person');

    const aliasSlotView = aliasList!.slotView();
    expect(aliasSlotView).to.not.be.undefined;
    expect(aliasSlotView!.name()).to.equal('aliases');
    const roleValue = instance.get('role');
    expect(roleValue).to.not.be.undefined;
    const roleSlotView = roleValue!.slotView();
    expect(roleSlotView).to.not.be.undefined;
    expect(roleSlotView!.name()).to.equal('role');

    const slotHandles = classViewHandle!.slotViews();
    const roleSlot = slotHandles.find((slot) => slot.name() === 'role');
    expect(roleSlot).to.not.equal(undefined);
    const roleInfos = roleSlot!.rangeInfos();
    expect(roleInfos.length).to.be.greaterThan(0);
    const inlineModes = roleInfos.map((info) => info.slotInlineMode());
    expect(inlineModes).to.include('primitive');

    const rawPlain = instance.toPlainJson() as unknown;
    const plainValue =
      typeof rawPlain === 'string'
        ? (JSON.parse(rawPlain) as { name: string; aliases: string[]; role: string })
        : (rawPlain as { name: string; aliases: string[]; role: string } | Map<string, unknown>);
    const plain =
      plainValue instanceof Map
        ? (Object.fromEntries(plainValue) as { name: string; aliases: string[]; role: string })
        : (plainValue as { name: string; aliases: string[]; role: string });
    expect(plain.name).to.equal('Alice');
    expect(plain.aliases).to.deep.equal(['Al']);
    expect(plain.role).to.equal('manager');

    const navigated = instance.navigate(['aliases', '0']);
    expect(navigated, 'navigate result').to.not.be.undefined;
    expect(navigated!.scalarValue()).to.equal('Al');
  });

  it('serializes instances to turtle', () => {
    const view = asset360.loadSchemaView(SCHEMA_YAML);
    const instance = view.loadInstanceFromJson('Person', PERSON_JSON);

    const ttl = instance.toTurtle();
    expect(ttl).to.be.a('string');
    expect(ttl).to.include('@prefix');
    expect(ttl).to.include('ex:name');
    expect(ttl).to.include('"Alice"');
  });

  it('inspects schema views via wasm', () => {
    const view = asset360.loadSchemaView(SCHEMA_YAML);

    const schemaId = view.primarySchemaId() ?? '';
    expect(schemaId).to.equal('https://example.org/test');

    const classIds = view.classIds();
    expect(classIds).to.include('Person');

    const enumIds = view.enumIds();
    expect(enumIds).to.include('PersonRole');

    const classView = view.classView(schemaId, 'Person');
    expect(classView).to.not.equal(undefined);
    expect(classView!.name()).to.equal('Person');

    const slotHandles = classView!.slotViews();
    const roleSlot = slotHandles.find((slot) => slot.name() === 'role');
    expect(roleSlot).to.not.equal(undefined);

    const slotView = view.slotView(schemaId, 'role');
    expect(slotView).to.not.equal(undefined);
    expect(slotView!.definition()).to.not.equal(null);

    const rangeEnum = roleSlot!.rangeEnum();
    expect(rangeEnum).to.not.equal(undefined);
    expect(rangeEnum!.name()).to.equal('PersonRole');

    const slotRangeEnum = slotView!.rangeEnum();
    expect(slotRangeEnum).to.not.equal(undefined);
    expect(slotRangeEnum!.name()).to.equal('PersonRole');

    const enumView = view.enumView(schemaId, 'PersonRole');
    expect(enumView).to.not.equal(undefined);
    expect(enumView!.permissibleValueKeys()).to.deep.equal(['manager']);
  });

  it('resolves schema imports via addSchemaStrWithImportRef', () => {
    const importerYaml = `id: https://example.org/importer
name: importer
imports:
  - https://example.org/personinfo.yaml
`;
    const view = asset360.loadSchemaView(importerYaml);
    const unresolvedBefore = view.getUnresolvedSchemaRefs() as Array<[string, string]>;
    expect(unresolvedBefore).to.deep.equal([
      ['https://example.org/importer', 'https://example.org/personinfo.yaml'],
    ]);

    const importYaml = `id: https://example.org/personinfo
name: personinfo
`;
    const inserted = view.addSchemaStrWithImportRef(
      importYaml,
      'https://example.org/importer',
      'https://example.org/personinfo.yaml',
    );
    expect(inserted).to.equal(true);

    const unresolvedAfter = view.getUnresolvedSchemaRefs() as Array<[string, string]>;
    expect(unresolvedAfter).to.deep.equal([]);

    expect(view.getResolutionUriOfSchema('https://example.org/personinfo')).to.equal(
      'https://example.org/personinfo.yaml',
    );
  });

  it('names list elements the way diff() addresses them', () => {
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const data = {
      name: 'svc',
      sections: [
        { sequenceNumber: 1, note: 'one' },
        { sequenceNumber: 2, note: 'two' },
      ],
      contacts: [
        { kind: 'home', primary: true, phone: '555-0100' },
        { kind: 'work', primary: false, phone: '555-0199' },
      ],
      notes: [{ body: 'first' }, { body: 'second' }],
    };
    const instance = view.loadInstanceFromJson('Service', JSON.stringify(data));

    // A single-slot `unique_keys` labels by the bare scalar value. "1" names
    // the FIRST element, so a positional answer here would land every rewrite
    // one element early and still look like a success.
    const sections = instance.get('sections')!;
    expect(sections.listPathSegments()).to.deep.equal(['1', '2']);
    expect(sections.at(0)!.elementIdentityLabel()).to.equal('1');
    expect(sections.at(1)!.elementIdentityLabel()).to.equal('2');

    // A composite `unique_keys` encodes a compact JSON array, values
    // stringified in `unique_key_slots` order — booleans lowercase. This is
    // the segment shape an emitter is likeliest to get subtly wrong alone.
    const contacts = instance.get('contacts')!;
    expect(contacts.listPathSegments()).to.deep.equal([
      '["home","true"]',
      '["work","false"]',
    ]);
    expect(contacts.at(0)!.elementIdentityLabel()).to.equal('["home","true"]');

    // A class declaring no identity at all is positional, and stays that way.
    const notes = instance.get('notes')!;
    expect(notes.listPathSegments()).to.deep.equal(['0', '1']);
    expect(notes.at(0)!.elementIdentityLabel()).to.equal(undefined);

    // listPathSegments only answers for lists.
    expect(instance.listPathSegments()).to.equal(undefined);
    expect(instance.get('name')!.listPathSegments()).to.equal(undefined);

    // The property that actually matters: the labels are the segments `diff()`
    // emits for the same data. Perturb one leaf per list and compare.
    const changed = JSON.parse(JSON.stringify(data)) as typeof data;
    changed.sections[1].note = 'TWO';
    changed.contacts[1].phone = '555-0200';
    changed.notes[1].body = 'SECOND';
    const deltas = view.diffJson(
      'Service',
      data,
      changed,
      false,
    ) as Array<{ path: string[] }>;

    expect(diffSegmentsUnder(deltas, 'sections')).to.deep.equal(['2']);
    expect(diffSegmentsUnder(deltas, 'contacts')).to.deep.equal(['["work","false"]']);
    expect(diffSegmentsUnder(deltas, 'notes')).to.deep.equal(['1']);
  });

  it('keeps per-element labels when one sibling has no identity', () => {
    // The case this whole design rests on. A row the user has just added has
    // its identity slot still empty, which flips the *list* to positional —
    // but the labelled rows must still report their own labels, or the table
    // loses every row's provenance the moment someone hits "add".
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const data = {
      name: 'svc',
      sections: [
        { sequenceNumber: 1, note: 'one' },
        { note: 'freshly added, no key yet' },
        { sequenceNumber: 3, note: 'three' },
      ],
    };
    const instance = view.loadInstanceFromJson('Service', JSON.stringify(data));
    const sections = instance.get('sections')!;

    // The whole list goes positional, because one element carries no label.
    expect(sections.listPathSegments()).to.deep.equal(['0', '1', '2']);

    // ...and yet the per-element rule is untouched.
    expect(sections.at(0)!.elementIdentityLabel()).to.equal('1');
    expect(sections.at(1)!.elementIdentityLabel()).to.equal(undefined);
    expect(sections.at(2)!.elementIdentityLabel()).to.equal('3');

    // diff() agrees: it addresses this list positionally too.
    const changed = JSON.parse(JSON.stringify(data)) as typeof data;
    changed.sections[2].note = 'THREE';
    const deltas = view.diffJson(
      'Service',
      data,
      changed,
      false,
    ) as Array<{ path: string[] }>;
    expect(diffSegmentsUnder(deltas, 'sections')).to.deep.equal(['2']);
  });
});

describe('delta bindings', () => {
  before(async () => {
    await readyAsset360();
  });

  const BASE = {
    name: 'svc',
    sections: [
      { sequenceNumber: 1, note: 'one' },
      { sequenceNumber: 2, note: 'two' },
    ],
    contacts: [{ kind: 'home', primary: true, phone: '555-0100' }],
  };
  const EDITED = {
    name: 'svc',
    sections: [
      { sequenceNumber: 1, note: 'one' },
      { sequenceNumber: 2, note: 'TWO' },
    ],
    contacts: [{ kind: 'home', primary: true, phone: '555-0200' }],
  };

  it('patchJson round-trips a diff and reports unresolvable paths', () => {
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const deltas = view.diffJson('Service', BASE, EDITED, false);

    const applied = view.patchJson('Service', BASE, deltas, false, true) as {
      value: unknown;
      failed: string[][];
    };
    expect(applied.failed).to.deep.equal([]);

    // The round trip is the property: patching the base with diff(base, edited)
    // must land exactly on edited, per the engine's own diff.
    expect(view.diffJson('Service', applied.value, EDITED, false)).to.deep.equal([]);

    // report-never-guess: an address resolving to nothing comes back named,
    // not silently skipped. A frontend that rendered an unapplied delta would
    // present stale data as current.
    const bogus = [
      { path: ['sections', '404', 'note'], op: 'update', old: 'two', new: 'nope' },
    ];
    const partial = view.patchJson('Service', BASE, bogus, false, true) as {
      value: unknown;
      failed: string[][];
    };
    expect(partial.failed).to.deep.equal([['sections', '404', 'note']]);
  });

  it('equals answers the unchanged question the same way diff does', () => {
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const a = view.loadInstanceFromJson('Service', JSON.stringify(BASE));
    const b = view.loadInstanceFromJson('Service', JSON.stringify(BASE));
    const c = view.loadInstanceFromJson('Service', JSON.stringify(EDITED));

    expect(a.equals(b, false)).to.equal(true);
    expect(a.equals(c, false)).to.equal(false);

    // The autosave gate's whole reason for existing: this boolean must never
    // disagree with "diff produced nothing".
    expect(a.equals(b, false)).to.equal(
      view.diffJson('Service', BASE, BASE, false).length === 0,
    );
    expect(a.equals(c, false)).to.equal(
      view.diffJson('Service', BASE, EDITED, false).length === 0,
    );
  });

  it('diffInstances matches diffJson and can diff a navigated subtree', () => {
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const base = view.loadInstanceFromJson('Service', JSON.stringify(BASE));
    const edited = view.loadInstanceFromJson('Service', JSON.stringify(EDITED));

    // Order-insensitively, deliberately: `diff` walks an object's slots from a
    // hash map, so deltas for *sibling slots* come back in no guaranteed order
    // and the two calls parse their inputs separately. The claim under test is
    // that the same edits are reported, not that they queue up the same way.
    const byPath = (ds: Array<{ path: string[] }>) =>
      [...ds].sort((x, y) => JSON.stringify(x.path).localeCompare(JSON.stringify(y.path)));

    expect(byPath(asset360.diffInstances(base, edited, false))).to.deep.equal(
      byPath(view.diffJson('Service', BASE, EDITED, false)),
    );

    // The point of taking handles: a navigated subtree can be diffed without
    // the caller knowing its class name, which `diffJson` would demand.
    const baseSections = base.navigate(['sections']);
    const editedSections = edited.navigate(['sections']);
    expect(baseSections, 'base sections').to.not.be.undefined;
    const subtree = asset360.diffInstances(
      baseSections!,
      editedSections!,
      false,
    ) as Array<{ path: string[] }>;
    expect(subtree.length).to.equal(1);
    // Paths are relative to the subtree that was diffed, not the root.
    expect(subtree[0].path).to.deep.equal(['2', 'note']);
  });

  it('diffJson can describe a changed key as an update instead of a replacement', () => {
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const before = { name: 'svc', cover: { sequence: 1, caption: 'hero' } };
    const after = { name: 'svc', cover: { sequence: 2, caption: 'hero' } };

    // Default stays today's behaviour: a changed key means a different thing,
    // so the element is replaced wholesale.
    const byDefault = view.diffJson('Service', before, after, false) as Array<{
      path: string[];
    }>;
    expect(byDefault.map((d) => d.path)).to.deep.equal([['cover']]);

    // Passing it explicitly must not change anything — that is what makes the
    // added parameter non-breaking for every existing caller.
    expect(view.diffJson('Service', before, after, false, true)).to.deep.equal(byDefault);

    // Opting out describes the edit in place. On a live edit form this is the
    // difference between the row updating and the row flashing away and back
    // mid-keystroke.
    const asUpdate = view.diffJson('Service', before, after, false, false) as Array<{
      path: string[];
      old: unknown;
      new: unknown;
    }>;
    expect(asUpdate.map((d) => d.path)).to.deep.equal([['cover', 'sequence']]);
    expect(asUpdate[0].old).to.equal(1);
    expect(asUpdate[0].new).to.equal(2);
  });

  it('leaves keyed list elements alone whatever the identifier flag says', () => {
    // The flag decides how a changed key is described once two objects are
    // paired. In a keyed list they are never paired: identity labelling splits
    // `sequence: 1` and `sequence: 2` into a removal and an addition first, so
    // the flag has nothing left to decide. Pinned because the obvious reading
    // of the option — "controls changed keys everywhere" — is wrong here.
    const view = asset360.loadSchemaView(IDENTITY_SCHEMA_YAML);
    const before = { name: 'svc', pictures: [{ sequence: 1, caption: 'hero' }] };
    const after = { name: 'svc', pictures: [{ sequence: 2, caption: 'hero' }] };

    const ops = (ds: Array<{ path: string[]; op: string }>) =>
      ds.map((d) => `${d.op} ${d.path.join('/')}`);

    const withFlag = view.diffJson('Service', before, after, false, true) as Array<{
      path: string[];
      op: string;
    }>;
    const withoutFlag = view.diffJson('Service', before, after, false, false) as Array<{
      path: string[];
      op: string;
    }>;

    expect(ops(withFlag)).to.deep.equal(['remove pictures/1', 'add pictures/2']);
    expect(ops(withoutFlag)).to.deep.equal(ops(withFlag));
  });

});

export {};

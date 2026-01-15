<script lang="ts">
  import Modal from "./Modal.svelte"
  import { getContext } from "svelte";
  import * as m from '$lib/paraglide/messages.js'
  import { ChartSave, ChartCtx } from "./chart";
  import type { Writable } from "svelte/store";
  import type { Chart, Nullable } from 'klinecharts';
  import { derived } from "svelte/store";
  import { IndFieldsMap, type IndField, type IndFieldOption, getIndFieldDefaultValue, inferIndFieldType, normalizeIndFieldOptions } from './coms';
  let { show = $bindable() } = $props();
  
  const ctx = getContext('ctx') as Writable<ChartCtx>;
  const save = getContext('save') as Writable<ChartSave>;
  const chart = getContext('chart') as Writable<Nullable<Chart>>;
  type FieldState = IndField & { value: string | number | boolean }
  let fields = $state<FieldState[]>([]);

  function buildFields(): FieldState[] {
    if (!$ctx.editIndName) return [];
    const baseFields = IndFieldsMap[$ctx.editIndName] ?? [];
    if (baseFields.length === 0) return [];
    const inds = $chart?.getIndicators({name: $ctx.editIndName, paneId: $ctx.editPaneId}) ?? [];
    const calcParams = inds[0]?.calcParams ?? [];
    return baseFields.map((field, index) => {
      const baseValue = calcParams[index] !== undefined ? calcParams[index] : field.default;
      const type = inferIndFieldType(field, baseValue);
      const value = baseValue !== undefined ? baseValue : getIndFieldDefaultValue({ ...field, type });
      return { ...field, type, value };
    });
  }

  function getOptions(field: FieldState): IndFieldOption[] {
    return normalizeIndFieldOptions(field.options);
  }

  function getStep(field: FieldState): string | number {
    if (field.step !== undefined) return field.step;
    if (field.precision !== undefined) {
      const factor = Math.pow(10, field.precision);
      return 1 / factor;
    }
    return 'any';
  }

  function inferSelectValueType(field: FieldState): 'number' | 'string' | 'boolean' {
    if (field.valueType) return field.valueType;
    if (typeof field.default === 'number') return 'number';
    if (typeof field.default === 'boolean') return 'boolean';
    const options = getOptions(field);
    if (options.length > 0) {
      const optValue = options[0].value;
      if (typeof optValue === 'number') return 'number';
      if (typeof optValue === 'boolean') return 'boolean';
    }
    if (typeof field.value === 'number') return 'number';
    if (typeof field.value === 'boolean') return 'boolean';
    return 'string';
  }

  function coerceFieldValue(field: FieldState): string | number | boolean {
    const fieldType = inferIndFieldType(field, field.value);
    if (fieldType === 'switch') return Boolean(field.value);
    if (fieldType === 'number') {
      const num = typeof field.value === 'number' ? field.value : Number(field.value);
      if (Number.isFinite(num)) return num;
      if (typeof field.default === 'number') return field.default;
      return 0;
    }
    if (fieldType === 'select') {
      const valueType = inferSelectValueType(field);
      if (valueType === 'number') {
        const num = Number(field.value);
        if (Number.isFinite(num)) return num;
        return typeof field.default === 'number' ? field.default : 0;
      }
      if (valueType === 'boolean') {
        if (field.value === true || field.value === false) return field.value as boolean;
        return `${field.value}` === 'true';
      }
      return `${field.value ?? ''}`;
    }
    return `${field.value ?? ''}`;
  }

  // 当编辑的指标改变时，更新参数
  const showEdit = derived(ctx, ($ctx) => $ctx.modalIndCfg)
  showEdit.subscribe(() => {
    if (!$ctx.modalIndCfg) {
      fields = [];
      return;
    }
    fields = buildFields();
  });

  function handleConfirm(from: string) {
    if (from !== 'confirm' || !$ctx.editIndName || !$ctx.editPaneId || fields.length === 0) {
      show = false;
      return;
    }
    
    const result = fields.map((field) => coerceFieldValue(field));

    console.log('overrideIndicator', $ctx.editIndName, $ctx.editPaneId, result)
    $chart?.overrideIndicator({
      name: $ctx.editIndName,
      calcParams: result,
      paneId: $ctx.editPaneId,
    });

    save.update(s => {
      s.saveInds[`${$ctx.editPaneId}_${$ctx.editIndName}`] = {
        name: $ctx.editIndName,
        params: result,
        pane_id: $ctx.editPaneId
      };
      return s
    })
    show = false;
  }

</script>

<Modal title={$ctx.editIndName} width={360} bind:show={show} click={handleConfirm}>
  {#if !fields.length}
    <div class="flex justify-center items-center min-h-[120px]">
      <div class="text-base-content/70">{m.no_ind_params()}</div>
    </div>
  {:else}
    <div class="grid grid-cols-5 gap-5 mt-5">
      {#each fields as field, i}
        <span class="col-span-2 text-base-content/70 flex items-center justify-center">{field.title}</span>
        {#if field.type === 'select'}
          <select class="col-span-3 select w-full" bind:value={field.value}>
            {#each getOptions(field) as opt}
              <option value={opt.value}>{opt.label}</option>
            {/each}
          </select>
        {:else if field.type === 'switch'}
          <div class="col-span-3 flex items-center">
            <input type="checkbox" class="toggle" bind:checked={field.value} />
          </div>
        {:else if field.type === 'text'}
          <input
            type="text"
            class="col-span-3 input w-full"
            bind:value={field.value}
            placeholder={field.placeholder ?? ''}
          />
        {:else}
          <input
            type="number"
            class="col-span-3 input w-full"
            bind:value={field.value}
            min={field.min}
            max={field.max}
            step={getStep(field)}
          />
        {/if}
      {/each}
    </div>
  {/if}
</Modal>

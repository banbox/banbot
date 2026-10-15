<script lang="ts" module>
  export type ThemeSpec = Record<string, StyleSpec>;
  export type StyleSpec = {
    [propOrSelector: string]: string | number | StyleSpec | null;
  };
</script>

<script lang="ts">
  import {
    highlightSpecialChars, drawSelection, highlightActiveLine, dropCursor,
    rectangularSelection, crosshairCursor,
    lineNumbers, highlightActiveLineGutter
  } from "@codemirror/view"
  import {
    defaultHighlightStyle, syntaxHighlighting, indentOnInput, bracketMatching,
    foldGutter, foldKeymap
  } from "@codemirror/language"
  import {defaultKeymap, history, historyKeymap} from "@codemirror/commands"
  import {searchKeymap, highlightSelectionMatches} from "@codemirror/search"
  import {autocompletion, completionKeymap, closeBrackets, closeBracketsKeymap} from "@codemirror/autocomplete"
  import {lintKeymap} from "@codemirror/lint"
  import {onDestroy, onMount} from 'svelte';
  import {EditorView, keymap, placeholder as placeholderExt} from '@codemirror/view';
  import {EditorState, StateEffect, type Extension, type Transaction} from '@codemirror/state';
  import {indentWithTab} from '@codemirror/commands';
  import {indentUnit, type LanguageSupport} from '@codemirror/language';
  import type {CompletionContext, CompletionResult} from '@codemirror/autocomplete';
  import {browser} from '$app/environment';
  import {go} from '@codemirror/lang-go';
  import {yaml} from '@codemirror/lang-yaml';
  import {type KeyBinding} from '@codemirror/view';
  import _ from 'lodash';
  import {createBanBotCompletionSource} from "$lib/dev/ban_hints";

  let element: HTMLDivElement | undefined = $state(undefined);
  let editor = $state<EditorView | null>(null);
  let value = $state('');
  let documentName = '';
  let lang: LanguageSupport | undefined = $state(go());
  let useTab = $state(true);
  let langCode = $state('');

  let update_from_prop = false;
  let first_config = true;

  let {
    theme,
    basic = true,
    extensions,
    class: classes,
    tabSize = 4,
    styles,
    lineWrapping = true,
    editable = true,
    readonly = false,
    placeholder,
    debounce_msecs = 1000,
    height = '100%',
    width = '100%',
    fontSize = 11,
    ready,
    change,
    reconfigure
  }: {
    theme?: Extension;
    basic?: boolean;
    extensions?: Extension[];
    class?: string;
    tabSize?: number;
    styles?: ThemeSpec;
    lineWrapping?: boolean;
    editable?: boolean;
    readonly?: boolean;
    placeholder?: string | HTMLElement | null | undefined;
    debounce_msecs?: number;
    height?: string;
    width?: string;
    fontSize?: number;
    ready?: (view: EditorView) => void;
    change?: (value: string, name?: string) => void;
    reconfigure?: (view: EditorView) => void;
  } = $props();


  // 监听value变化，根据内容设置语言
  $effect(() => {
    if (value) {
      const funcRegex = /\bfunc\s/;
      if (funcRegex.test(value)) {
        langCode = 'go';
      } else {
        langCode = 'yml';
      }
    }
  });

  // 自定义自动补全
  const customCompletions = (context: CompletionContext): CompletionResult | null => {
    if (langCode == 'go') {
      return createBanBotCompletionSource(context);
    } else {
      console.log('unsupport lang:', langCode);
    }

    return null;
  };

  const basicExts: Extension[] = [
    lineNumbers(),
    highlightActiveLineGutter(),
    highlightSpecialChars(),
    history(),
    foldGutter(),
    drawSelection(),
    dropCursor(),
    EditorState.allowMultipleSelections.of(true),
    indentOnInput(),
    syntaxHighlighting(defaultHighlightStyle, {fallback: true}),
    bracketMatching(),
    closeBrackets(),
    autocompletion(),
    rectangularSelection(),
    crosshairCursor(),
    highlightActiveLine(),
    highlightSelectionMatches(),
    keymap.of([
      ...closeBracketsKeymap,
      ...defaultKeymap,
      ...searchKeymap,
      ...historyKeymap,
      ...foldKeymap,
      ...completionKeymap,
      ...lintKeymap
    ] as KeyBinding[])
  ]

  // 自定义自动补全扩展
  const autocompleteExt = autocompletion({
    override: [customCompletions],
    defaultKeymap: true,
    closeOnBlur: true,
  });

  // 监听 fontSize 的变化
  $effect(() => {
    if (!editor || !fontSize) return;
    editor.dispatch({
      effects: StateEffect.reconfigure.of(calcExtensions())
    });
  });

  function calcExtensions(): Extension[] {
    const exts: Extension[] = [];

    // 基础扩展
    if (basic) exts.push(...basicExts);

    // 字体大小主题
    exts.push(EditorView.theme({
      ".cm-content": {fontSize: `${fontSize}pt`},
      ".cm-gutters": {fontSize: `${fontSize}pt`},
    }));

    // 缩进和Tab行为
    exts.push(indentUnit.of(' '.repeat(tabSize)));
    if (useTab) exts.push(keymap.of([indentWithTab]));

    // 编辑状态
    exts.push(EditorState.readOnly.of(readonly));
    exts.push(EditorView.editable.of(editable));

    // 自定义自动补全
    exts.push(autocompleteExt);

    // 占位符
    if (placeholder) exts.push(placeholderExt(placeholder));

    // 语言支持
    if (lang) exts.push(lang);

    // 换行行为
    if (lineWrapping) exts.push(EditorView.lineWrapping);

    // 主题和样式
    if (styles) exts.push(EditorView.theme(styles));
    if (theme) exts.push(theme);

    // 额外的扩展
    if (extensions) exts.push(...extensions);

    if (editor) {
      if (first_config) {
        first_config = false;
        return exts;
      }

      editor.dispatch({
        effects: StateEffect.reconfigure.of(exts)
      });

      reconfigure?.(editor);
    }
    return exts;
  }

  function create_editor_state(value: string | null | undefined, exts: Extension[]): EditorState {
    return EditorState.create({
      doc: value ?? undefined,
      extensions: exts
    });
  }

  const debouncedChange = _.debounce((text: string, name: string) => {
    change?.(text, name);
  }, 1000, {trailing: true});

  onMount(() => {
    const exts = calcExtensions();
    editor = new EditorView({
      parent: element,
      state: create_editor_state(value, exts),
      dispatch(transaction: Transaction) {
        if(!editor)return
        editor.update([transaction]);

        if (!update_from_prop && change && transaction.docChanged) {
          const text = editor.state.doc.toString();
          if (debounce_msecs <= 0) {
            change(text, documentName);
          } else {
            debouncedChange(text, documentName);
          }
        }
      }
    });
    ready?.(editor);
  });

  onDestroy(() => {
    debouncedChange.flush();
    editor?.destroy();
    debouncedChange.cancel();
  });

  export function setValue(name: string, text: string): void {
    if (!editor)return;
    debouncedChange.flush();
    documentName = name;

    // 这里将text按换行切分，如果行首是\t的行的数量超过行首空格的数量，则将useTab设置为true
    const lines = text.split('\n');
    const tabCount = lines.filter(line => line.startsWith('\t')).length;
    const spaceCount = lines.filter(line => line.startsWith(' ')).length;
    useTab = tabCount > spaceCount;

    const ext = name.split('.').pop()?.toLowerCase() || '';
    if (ext === 'go') {
      lang = go();
      langCode = 'go';
    } else if (ext === 'yaml' || ext === 'yml') {
      lang = yaml();
      langCode = 'yml';
    } else {
      lang = undefined;
    }
    value = text;

    update_from_prop = true;
    try {
      // 更新编辑器内容
      editor.dispatch({
        changes: {
          from: 0,
          to: editor.state.doc.length,
          insert: value,
        }
      });

      // 更新编辑器配置
      const exts = calcExtensions();
      const newState = create_editor_state(value, exts);
      editor.setState(newState);

      // 调用重新配置回调
      if (reconfigure) reconfigure(editor);
    } catch (error) {
      console.error(`CodeMirror init fail:`, error);
    } finally {
      update_from_prop = false;
    }
  }

  export function flushChanges(): void {
    debouncedChange.flush();
  }

  // 获取编辑器内容
  export function getValue(): string {
    if (!editor) return '';
    return editor.state.doc.toString();
  }

  // 格式化代码
  function formatCode(): void {
    if (!editor) return;
    const doc = editor.state.doc;
    const formatted = doc.toString().trim();

    editor.dispatch({
      changes: {
        from: 0,
        to: doc.length,
        insert: formatted
      }
    });
  }
</script>

{#if browser}
    <div class="codemirror-wrapper {classes}" bind:this={element} style="height: {height}; width: {width};"></div>
{:else}
    <div class="scm-waiting {classes}">
        <div class="scm-waiting__loading scm-loading">
            <div class="scm-loading__spinner"></div>
            <p class="scm-loading__text">Loading editor...</p>
        </div>

        <pre class="scm-pre cm-editor">{value}</pre>
    </div>
{/if}

<style>
    .codemirror-wrapper {
        outline: none;
        height: 100%;
        overflow-y: auto;
    }

    :global(.codemirror-wrapper .cm-editor) {
        height: 100%;
    }

    :global(.codemirror-wrapper .cm-scroller) {
        overflow: auto;
    }

    .scm-waiting {
        position: relative;
    }

    .scm-waiting__loading {
        position: absolute;
        top: 0;
        left: 0;
        bottom: 0;
        right: 0;
        background-color: rgba(255, 255, 255, 0.5);
    }

    .scm-loading {
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .scm-loading__spinner {
        width: 1rem;
        height: 1rem;
        border-radius: 100%;
        border: solid 2px #000;
        border-top-color: transparent;
        margin-right: 0.75rem;
        animation: spin 1s linear infinite;
    }

    .scm-loading__text {
        font-family: sans-serif;
    }

    .scm-pre {
        font-size: 0.85rem;
        font-family: monospace;
        tab-size: 2;
        -moz-tab-size: 2;
        resize: none;
        pointer-events: none;
        user-select: none;
        overflow: auto;
    }

    @keyframes spin {
        0% {
            transform: rotate(0deg);
        }
        100% {
            transform: rotate(360deg);
        }
    }
</style>
